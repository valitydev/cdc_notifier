-module(cdc_progressor).

-behaviour(gen_server).

%% API
-export([start_link/3]).
-export([child_spec/3]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% wal_reader callbacks
-export([handle_replication_data/2]).
-export([handle_replication_stop/2]).

-export_type([stream_config/0]).
-export_type([message_data/0]).

-type state() :: #{_ => _}.
-type namespace_id() :: atom().
-type stream_config() :: #{
    kafka_client := atom(),
    eventsink_topic := binary(),
    lifecycle_topic := binary()
}.
-type streams() :: #{
    DataSource :: namespace_id() := DataDestination :: stream_config()
}.
-type message_data() :: #{
    key := binary(),
    value := EncodedPayload :: binary()
}.

-define(DEFAULT_RESEND_TIMEOUT, 3000).
-define(DEFAULT_MAX_RETRIES, 3).
-define(DEFAULT_WAL_RECONNECT_TIMEOUT, 5000).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(epg_connector_app:db_opts(), ReplSlotName :: string(), Streams :: streams()) ->
    {ok, pid()} | ignore | {error, term()}.
start_link(DbOpts, ReplSlot, Streams) ->
    gen_server:start_link(?MODULE, [DbOpts, ReplSlot, Streams], []).

-spec child_spec(DbOpts :: map(), ReplSlotName :: string(), Streams :: streams()) ->
    supervisor:child_spec().
child_spec(DbOpts, ReplSlot, Streams) ->
    #{
        id => "CDC_PRG_" ++ ReplSlot,
        start => {?MODULE, start_link, [DbOpts, ReplSlot, Streams]}
    }.

-spec handle_replication_data(
    pid(),
    ReplData :: [{Table :: binary(), Operation :: insert | update | delete, Row :: map(), OldRow :: map()}]
) -> ok.
handle_replication_data(Pid, ReplData) ->
    gen_server:call(Pid, {handle_replication_data, ReplData}, infinity).

-spec handle_replication_stop(_, _) -> ok.
handle_replication_stop(Pid, ReplStop) ->
    gen_server:cast(Pid, {handle_replication_stop, ReplStop}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init([any()]) -> {ok, state()}.
init([DbOpts, ReplSlot, Streams]) ->
    NsIDs = maps:keys(Streams),
    {ok, Connection} = epgsql:connect(DbOpts),
    Publications = lists:foldl(
        fun(NsID, Acc) ->
            {ok, PubName} = create_publication_if_not_exists(Connection, NsID),
            [PubName | Acc]
        end,
        [],
        NsIDs
    ),
    ok = epgsql:close(Connection),
    Options = #{slot_type => persistent},
    {ok, Reader} =
        case epg_wal_reader:subscribe({?MODULE, self()}, DbOpts, ReplSlot, Publications, Options) of
            {ok, _} = OK ->
                OK;
            {error, {already_started, Pid}} ->
                {ok, Pid}
        end,
    MonitorRef = erlang:monitor(process, Reader),
    {ok, #{
        db_opts => DbOpts,
        repl_slot => ReplSlot,
        publications => Publications,
        wal_reader => Reader,
        streams => Streams,
        monitor => MonitorRef
    }}.

-spec handle_call(term(), {pid(), term()}, state()) ->
    {reply, term(), state()}.
handle_call({handle_replication_data, ReplData}, _From, State) ->
    Data = parse_repl_data(ReplData, State),
    Reply = send_with_retry(Data),
    %% if sends fail (reply=error) then epg_wal_reader will be crashed
    %% after reconnect_timeout wal will be reconnected and send will be retried
    {reply, Reply, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast({handle_replication_stop, _ReplSlot}, #{wal_reader := undefined} = State) ->
    %% already handled via monitor
    {noreply, State};
handle_cast({handle_replication_stop, _ReplSlot}, #{wal_reader := ReaderPid} = State) when is_pid(ReaderPid) ->
    ReconnectTimeout = application:get_env(cdc_progressor, reconnect_timeout, ?DEFAULT_WAL_RECONNECT_TIMEOUT),
    erlang:start_timer(ReconnectTimeout, self(), restart_replication),
    {noreply, State#{monitor => undefined, wal_reader => undefined}};
handle_cast(_Msg, State) ->
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info(
    {'DOWN', MonitorRef, _Type, ReaderPid, _Info},
    #{monitor := MonitorRef, wal_reader := ReaderPid} = State
) ->
    %% wal reader crashed (unexpected down)
    ReconnectTimeout = application:get_env(cdc_progressor, reconnect_timeout, ?DEFAULT_WAL_RECONNECT_TIMEOUT),
    erlang:start_timer(ReconnectTimeout, self(), restart_replication),
    {noreply, State#{monitor => undefined, wal_reader => undefined}};
handle_info({timeout, _TRef, restart_replication}, State) ->
    #{
        db_opts := DbOpts,
        repl_slot := ReplSlot,
        publications := Publications
    } = State,
    Options = #{slot_type => persistent},
    maybe
        {ok, Reader} ?= epg_wal_reader:subscribe({?MODULE, self()}, DbOpts, ReplSlot, Publications, Options),
        MonitorRef = erlang:monitor(process, Reader),
        NewState = State#{
            wal_reader => Reader,
            monitor => MonitorRef
        },
        {noreply, NewState}
    else
        Error ->
            logger:error("Can`t restart replication with error: ~p", [Error]),
            ReconnectTimeout = application:get_env(cdc_progressor, reconnect_timeout, ?DEFAULT_WAL_RECONNECT_TIMEOUT),
            erlang:start_timer(ReconnectTimeout, self(), restart_replication),
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

-spec terminate(term(), state()) -> term().
terminate(_Reason, #{wal_reader := Reader}) when is_pid(Reader) ->
    ok = epg_wal_reader:unsubscribe(Reader);
terminate(_Reason, _State) ->
    ok.

-spec code_change(term() | {down, term()}, state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

create_publication_if_not_exists(Connection, NsID) ->
    PubName = erlang:atom_to_list(NsID),
    PubNameEscaped = "\"" ++ PubName ++ "\"",
    #{
        processes := ProcessesTable,
        events := EventsTable
    } = tables(NsID),
    {ok, _, [{IsPublicationExists}]} = epgsql:equery(
        Connection,
        "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)",
        [PubName]
    ),
    case IsPublicationExists of
        true ->
            {ok, PubName};
        false ->
            {ok, _, _} = epgsql:equery(
                Connection,
                "CREATE PUBLICATION " ++ PubNameEscaped ++
                    " FOR TABLE " ++ ProcessesTable ++ " , " ++ EventsTable
            ),
            %% TODO delete after rework progressor
            {ok, _, _} = epgsql:equery(Connection, "ALTER TABLE " ++ ProcessesTable ++ " REPLICA IDENTITY FULL"),
            {ok, PubName}
    end.

tables(NsId) ->
    #{
        processes => construct_table_name(NsId, "_processes"),
        tasks => construct_table_name(NsId, "_tasks"),
        schedule => construct_table_name(NsId, "_schedule"),
        running => construct_table_name(NsId, "_running"),
        events => construct_table_name(NsId, "_events")
    }.

construct_table_name(NsId, Postfix) ->
    "\"" ++ erlang:atom_to_list(NsId) ++ Postfix ++ "\"".

-spec parse_repl_data(
    [{Table :: binary(), Op :: insert | update | delete, Row :: map(), PrevRow :: map()}],
    state()
) ->
    [
        {
            KafkaClient :: atom(),
            KafkaTopic :: binary(),
            EventKey :: binary(),
            Batch :: [message_data()]
        }
        | []
    ].
parse_repl_data(ReplData, State) ->
    lists:foldr(
        fun(ReplUnit, Acc) ->
            [parse_repl_unit(ReplUnit, State) | Acc]
        end,
        [],
        ReplData
    ).

parse_repl_unit({Table, Action, Row, PrevRow}, #{streams := Streams} = _State) ->
    %% see table naming convention in progressor (prg_pg_migration)
    [NsBin, TableType] = string:split(Table, <<"_">>, trailing),
    NsID = binary_to_atom(NsBin),
    StreamConfig = maps:get(NsID, Streams),

    case TableType of
        <<"processes">> ->
            handle_processes_change(NsBin, Action, Row, PrevRow, StreamConfig);
        <<"events">> when Action =:= insert ->
            handle_events_insert(NsBin, Row, StreamConfig);
        _Other ->
            %% not relevant table
            []
    end.

handle_processes_change(NsBin, Action, Row, PrevRow, StreamConfig) ->
    #{kafka_client := KafkaClient, lifecycle_topic := Topic} = StreamConfig,
    ProcessID = maps:get(<<"process_id">>, Row, <<>>),
    EventKey = cdc_prg_converter:event_key(NsBin, ProcessID),

    case convert_process_change(NsBin, Action, Row, PrevRow) of
        [] ->
            [];
        Batch ->
            {KafkaClient, Topic, EventKey, Batch}
    end.

convert_process_change(NsBin, insert, #{<<"process_id">> := ProcessID}, _PrevRow) ->
    cdc_prg_converter:convert_lifecycle_event(NsBin, ProcessID, init);
convert_process_change(NsBin, update, Row, PrevRow) ->
    #{<<"process_id">> := ProcessID} = Row,
    CurrentStatus = maps:get(<<"status">>, Row, undefined),
    PreviousStatus = maps:get(<<"status">>, PrevRow, undefined),

    case {PreviousStatus, CurrentStatus} of
        {<<"running">>, <<"error">>} ->
            ErrorReason = maps:get(<<"detail">>, Row, <<"unknown">>),
            cdc_prg_converter:convert_lifecycle_event(NsBin, ProcessID, {error, ErrorReason});
        {<<"error">>, <<"running">>} ->
            cdc_prg_converter:convert_lifecycle_event(NsBin, ProcessID, repair);
        _NoRelevantChange ->
            []
    end;
convert_process_change(NsBin, delete, #{<<"process_id">> := ProcessID}, _PrevRow) ->
    cdc_prg_converter:convert_lifecycle_event(NsBin, ProcessID, remove).

handle_events_insert(NsBin, Row, StreamConfig) ->
    #{kafka_client := KafkaClient, eventsink_topic := Topic} = StreamConfig,
    ProcessID = maps:get(<<"process_id">>, Row, <<>>),
    EventKey = cdc_prg_converter:event_key(NsBin, ProcessID),
    Batch = cdc_prg_converter:convert_eventsink_event(NsBin, Row),
    {KafkaClient, Topic, EventKey, Batch}.

%% kafka message publishing

send_with_retry(Data) ->
    ResendTimeout = application:get_env(cdc_progressor, resend_timeout, ?DEFAULT_RESEND_TIMEOUT),
    MaxRetries = application:get_env(cdc_progressor, max_retries, ?DEFAULT_MAX_RETRIES),
    send_with_retry(Data, ResendTimeout, MaxRetries, 0).

send_with_retry([], _ResendTimeout, _MaxRetries, _RetryCount) ->
    ok;
send_with_retry([[] | Rest], ResendTimeout, MaxRetries, RetryCount) ->
    send_with_retry(Rest, ResendTimeout, MaxRetries, RetryCount);
send_with_retry(_Data, _ResendTimeout, MaxRetries, RetryCount) when RetryCount > MaxRetries ->
    error;
send_with_retry([{KafkaClient, Topic, EventKey, Batch} | Rest] = Data, ResendTimeout, MaxRetries, RetryCount) ->
    try produce(KafkaClient, Topic, EventKey, Batch) of
        ok ->
            send_with_retry(Rest, ResendTimeout, MaxRetries, 0);
        {error, Reason} ->
            logger:error("kafka client produce error: ~p", [Reason]),
            send_with_retry(Data, ResendTimeout, MaxRetries, RetryCount + 1)
    catch
        Class:Reason:Stacktrace ->
            logger:error("kafka client produce exception: ~p", [[Class, Reason, Stacktrace]]),
            send_with_retry(Data, ResendTimeout, MaxRetries, RetryCount + 1)
    end.

produce(Client, Topic, PartitionKey, Batch) ->
    case brod:get_partitions_count(Client, Topic) of
        {ok, PartitionsCount} ->
            Partition = partition(PartitionsCount, PartitionKey),
            case brod:produce_sync_offset(Client, Topic, Partition, PartitionKey, Batch) of
                {ok, _Offset} ->
                    ok;
                {error, _Reason} = Error ->
                    Error
            end;
        {error, _Reason} = Error ->
            Error
    end.

partition(PartitionsCount, Key) ->
    erlang:phash2(Key) rem PartitionsCount.
