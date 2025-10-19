-module(cdc_prg_base_SUITE).

-include("cdc_prg_ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("kafka_protocol/include/kpro_public.hrl").
-include_lib("mg_proto/include/mg_proto_lifecycle_sink_thrift.hrl").
-include_lib("mg_proto/include/mg_proto_event_sink_thrift.hrl").

%% API
-export([
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    init_per_testcase/2,
    end_per_testcase/2,
    all/0,
    groups/0
]).

%% Tests
-export([simple_success_test/1]).
-export([lifecycle_sink_test/1]).
-export([db_connection_lost_test/1]).

-define(NS, 'namespace/subspace').

-define(LIFECYCLE_CREATED_MSG(NsBin, ID), #mg_lifesink_LifecycleEvent{
    machine_ns = NsBin,
    machine_id = ID,
    data = {machine, {created, #mg_lifesink_MachineLifecycleCreatedEvent{}}}
}).

-define(LIFECYCLE_FAILED_MSG(NsBin, ID), #mg_lifesink_LifecycleEvent{
    machine_ns = NsBin,
    machine_id = ID,
    data =
        {machine,
            {status_changed, #mg_lifesink_MachineLifecycleStatusChangedEvent{
                new_status = {failed, #mg_stateproc_MachineStatusFailed{}}
            }}}
}).

-define(LIFECYCLE_WORKING_MSG(NsBin, ID), #mg_lifesink_LifecycleEvent{
    machine_ns = NsBin,
    machine_id = ID,
    data =
        {machine,
            {status_changed, #mg_lifesink_MachineLifecycleStatusChangedEvent{
                new_status = {working, #mg_stateproc_MachineStatusWorking{}}
            }}}
}).

-define(EVENT_MSG(NsBin, ID, EventID),
    {event, #mg_evsink_MachineEvent{
        source_ns = NsBin,
        source_id = ID,
        event_id = EventID,
        data = {bin, <<EventID>>},
        format_version = 1
    }}
).

init_per_suite(Config) ->
    Apps = [brod, epg_connector, progressor, cdc_progressor],
    lists:foreach(fun(Application) -> ok = cdc_prg_ct_helper:start_app(Application) end, Apps),
    Config.

end_per_suite(_Config) ->
    Apps = lists:reverse([brod, epg_connector, progressor, cdc_progressor]),
    lists:foreach(fun(Application) -> ok = cdc_prg_ct_helper:stop_app(Application) end, Apps),
    ok.

init_per_group(_, C) ->
    C.

end_per_group(_, _) ->
    ok.

init_per_testcase(_, C) ->
    ok = cdc_prg_ct_helper:create_kafka_topics(),
    timer:sleep(500),
    C.

end_per_testcase(_, _) ->
    ok = cdc_prg_ct_helper:delete_kafka_topics(),
    timer:sleep(500),
    ok.

all() ->
    [
        {group, base}
    ].

groups() ->
    [
        {base, [], [
            simple_success_test,
            lifecycle_sink_test,
            db_connection_lost_test
        ]}
    ].

-spec simple_success_test(_) -> _.
simple_success_test(_C) ->
    _ = mock_processor(simple_success_test),
    ID = gen_id(),

    %% start process
    {ok, ok} = progressor:init(#{ns => ?NS, id => ID, args => <<"init_args">>}),
    NsBin = erlang:atom_to_binary(?NS),
    {ok,
        {OffsetLC1, [
            ?LIFECYCLE_CREATED_MSG(NsBin, ID)
        ]}} = read_kafka_messages(?BROKERS, ?CDC_LIFECYCLE_TOPIC),
    {ok,
        {OffsetES1, [
            ?EVENT_MSG(NsBin, ID, 1),
            ?EVENT_MSG(NsBin, ID, 2)
        ]}} = read_kafka_messages(?BROKERS, ?CDC_EVENTSINK_TOPIC),

    %% call process
    {ok, <<"response">>} = progressor:call(#{ns => ?NS, id => ID, args => <<"call_args">>}),
    {ok,
        {OffsetES2, [
            ?EVENT_MSG(NsBin, ID, 3),
            ?EVENT_MSG(NsBin, ID, 4)
        ]}} = read_kafka_messages(?BROKERS, ?CDC_EVENTSINK_TOPIC, OffsetES1),

    %% check topics
    {ok, {_, []}} = read_kafka_messages(?BROKERS, ?CDC_EVENTSINK_TOPIC, OffsetES2),
    {ok, {_, []}} = read_kafka_messages(?BROKERS, ?CDC_LIFECYCLE_TOPIC, OffsetLC1),
    unmock_processor(),
    ok.

-spec lifecycle_sink_test(_) -> _.
lifecycle_sink_test(_C) ->
    _ = mock_processor(lifecycle_sink_test),
    ID = gen_id(),

    %% start process
    {ok, ok} = progressor:init(#{ns => ?NS, id => ID, args => <<"init_args">>}),
    NsBin = erlang:atom_to_binary(?NS),
    {ok,
        {OffsetLC1, [
            ?LIFECYCLE_CREATED_MSG(NsBin, ID)
        ]}} = read_kafka_messages(?BROKERS, ?CDC_LIFECYCLE_TOPIC),

    %% call process with error
    {error, <<"call_error">>} = progressor:call(#{ns => ?NS, id => ID, args => <<"call_args">>}),
    {ok,
        {OffsetLC2, [
            ?LIFECYCLE_FAILED_MSG(NsBin, ID)
        ]}} = read_kafka_messages(?BROKERS, ?CDC_LIFECYCLE_TOPIC, OffsetLC1),

    %% repair failed (check for duplicates)
    {error, <<"repair_error">>} = progressor:repair(#{
        ns => ?NS, id => ID, args => <<"bad_repair_args">>
    }),
    {ok, {OffsetLC3, []}} = read_kafka_messages(?BROKERS, ?CDC_LIFECYCLE_TOPIC, OffsetLC2),

    %% repair success
    {ok, ok} = progressor:repair(#{ns => ?NS, id => ID, args => <<"repair_args">>}),
    {ok,
        {OffsetLC4, [
            ?LIFECYCLE_WORKING_MSG(NsBin, ID)
        ]}} = read_kafka_messages(?BROKERS, ?CDC_LIFECYCLE_TOPIC, OffsetLC3),
    {ok,
        {OffsetES1, [
            ?EVENT_MSG(NsBin, ID, 1)
        ]}} = read_kafka_messages(?BROKERS, ?CDC_EVENTSINK_TOPIC),

    %% check topics
    {ok, {_, []}} = read_kafka_messages(?BROKERS, ?CDC_EVENTSINK_TOPIC, OffsetES1),
    {ok, {_, []}} = read_kafka_messages(?BROKERS, ?CDC_LIFECYCLE_TOPIC, OffsetLC4),
    unmock_processor(),
    ok.

-spec db_connection_lost_test(_) -> _.
db_connection_lost_test(_C) ->
    %% stop CDC
    ok = cdc_prg_ct_helper:stop_app(cdc_progressor),
    _ = mock_processor(lifecycle_sink_test),
    ID = gen_id(),
    NsBin = erlang:atom_to_binary(?NS),

    %% start process
    {ok, ok} = progressor:init(#{ns => ?NS, id => ID, args => <<"init_args">>}),
    %% call process with error
    {error, <<"call_error">>} = progressor:call(#{ns => ?NS, id => ID, args => <<"call_args">>}),
    %% repair success
    {ok, ok} = progressor:repair(#{ns => ?NS, id => ID, args => <<"repair_args">>}),

    %% check topics
    {ok, {OffsetES1, []}} = read_kafka_messages(?BROKERS, ?CDC_EVENTSINK_TOPIC),
    {ok, {OffsetLC1, []}} = read_kafka_messages(?BROKERS, ?CDC_LIFECYCLE_TOPIC),

    %% Start CDC
    ok = cdc_prg_ct_helper:start_app(cdc_progressor),

    %% Read messages
    {ok,
        {_, [
            ?LIFECYCLE_CREATED_MSG(NsBin, ID),
            ?LIFECYCLE_FAILED_MSG(NsBin, ID),
            ?LIFECYCLE_WORKING_MSG(NsBin, ID)
        ]}} = read_kafka_messages(?BROKERS, ?CDC_LIFECYCLE_TOPIC, OffsetES1),
    {ok,
        {_, [
            ?EVENT_MSG(NsBin, ID, 1)
        ]}} = read_kafka_messages(?BROKERS, ?CDC_EVENTSINK_TOPIC, OffsetLC1),
    unmock_processor(),
    ok.

%% Internal functions
gen_id() ->
    base64:encode(crypto:strong_rand_bytes(8)).

event(Id) ->
    #{
        event_id => Id,
        timestamp => erlang:system_time(second),
        metadata => #{<<"format_version">> => 1},
        %% msg_pack compatibility for mg_proto
        payload => erlang:term_to_binary({bin, <<Id>>})
    }.

read_kafka_messages(Hosts, Topic) ->
    read_kafka_messages(Hosts, Topic, 0).

read_kafka_messages(Hosts, Topic, Offset) ->
    {ok, PartitionsCount} = brod:get_partitions_count(?DEFAULT_KAFKA_CLIENT, Topic),
    read_kafka_messages(Hosts, Topic, PartitionsCount - 1, Offset, []).

read_kafka_messages(_Hosts, _Topic, Partition, Offset, Result) when Partition < 0 ->
    {ok, {Offset, lists:reverse(Result)}};
read_kafka_messages(Hosts, Topic, Partition, Offset, Result) ->
    case brod:fetch(Hosts, Topic, Partition, Offset) of
        {ok, {Offset, []}} ->
            %% read from next partition
            read_kafka_messages(Hosts, Topic, Partition - 1, Offset, Result);
        {ok, {NewOffset, Records}} when NewOffset =/= Offset ->
            DeserializeFun = deserialize_fun(Topic),
            NewRecords = lists:reverse(
                [DeserializeFun(Value) || #kafka_message{value = Value} <- Records]
            ),
            read_kafka_messages(Hosts, Topic, Partition, NewOffset, NewRecords ++ Result)
    end.

deserialize_fun(?CDC_LIFECYCLE_TOPIC) ->
    fun deserialize_lifecycle/1;
deserialize_fun(?PRG_LIFECYCLE_TOPIC) ->
    fun deserialize_lifecycle/1;
deserialize_fun(?CDC_EVENTSINK_TOPIC) ->
    fun deserialize_eventsink/1;
deserialize_fun(?PRG_EVENTSINK_TOPIC) ->
    fun deserialize_eventsink/1;
deserialize_fun(_) ->
    fun(V) -> V end.

deserialize_lifecycle(Value) ->
    Type = {struct, struct, {mg_proto_lifecycle_sink_thrift, 'LifecycleEvent'}},
    {ok, Data, _} = thrift_strict_binary_codec:read(Value, Type),
    Data.

deserialize_eventsink(Value) ->
    Type = {struct, union, {mg_proto_event_sink_thrift, 'SinkEvent'}},
    {ok, Data, _} = thrift_strict_binary_codec:read(Value, Type),
    Data.

mock_processor(simple_success_test = TestCase) ->
    %Self = self(),
    MockProcessor = fun
        ({init, <<"init_args">>, _Process}, _Opts, _Ctx) ->
            Result = #{
                events => [event(1), event(2)]
            },
            %Self ! 1,
            {ok, Result};
        ({call, <<"call_args">>, _Process}, _Opts, _Ctx) ->
            Result = #{
                response => <<"response">>,
                events => [event(3)],
                action => #{set_timer => erlang:system_time(second)}
            },
            %Self ! 2,
            {ok, Result};
        ({timeout, <<>>, #{history := History} = _Process}, _Opts, _Ctx) ->
            %% timeout after call processing
            ?assertEqual(3, erlang:length(History)),
            Result = #{
                events => [event(4)]
            },
            %Self ! 3,
            {ok, Result}
    end,
    mock_processor(TestCase, MockProcessor);
mock_processor(TestCase) when
    TestCase =:= lifecycle_sink_test;
    TestCase =:= db_connection_lost_test
->
    %Self = self(),
    MockProcessor = fun
        ({init, <<"init_args">>, _Process}, _Opts, _Ctx) ->
            Result = #{
                events => []
            },
            %Self ! 1,
            {ok, Result};
        ({call, <<"call_args">>, #{history := []} = _Process}, _Opts, _Ctx) ->
            %Self ! 2,
            {error, <<"call_error">>};
        ({repair, <<"bad_repair_args">>, #{history := []} = _Process}, _Opts, _Ctx) ->
            %% repair error should not rewrite process detail
            %Self ! 3,
            {error, <<"repair_error">>};
        ({repair, <<"repair_args">>, #{history := []} = _Process}, _Opts, _Ctx) ->
            Result = #{
                events => [event(1)]
            },
            %Self ! 4,
            {ok, Result}
    end,
    mock_processor(TestCase, MockProcessor).

mock_processor(_TestCase, MockFun) ->
    meck:new(cdc_prg_processor),
    meck:expect(cdc_prg_processor, process, MockFun).

unmock_processor() ->
    meck:unload(cdc_prg_processor).
