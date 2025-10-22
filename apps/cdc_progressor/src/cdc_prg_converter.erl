-module(cdc_prg_converter).

-include_lib("mg_proto/include/mg_proto_lifecycle_sink_thrift.hrl").
-include_lib("mg_proto/include/mg_proto_event_sink_thrift.hrl").

-export([convert_lifecycle_event/3]).
-export([convert_eventsink_event/2]).
-export([event_key/2]).

-type lifecycle_event() ::
    init
    | repair
    | remove
    | {error, Reason :: binary()}.

-type processor_event() :: #{
    %% #{
    %%     <<"process_id">> := binary(),
    %%     <<"event_id">> := integer(),
    %%     <<"timestamp">> := calendar:datetime(),
    %%     <<"payload">> := Bytea :: binary(),
    %%     <<"metadata">> => #{<<"format_version">> => integer()}
    %% }
    Key :: binary() := Value :: term()
}.

-define(EPOCH_DIFF, 62167219200).

-spec convert_lifecycle_event(Namespace :: binary(), ProcessID :: binary(), lifecycle_event()) ->
    [cdc_progressor:message_data()].
convert_lifecycle_event(Namespace, ProcessID, Event) ->
    encode(fun serialize_lifecycle/3, Namespace, ProcessID, [lifecycle_event(Event)]).

-spec convert_eventsink_event(Namespace :: binary(), processor_event()) ->
    [cdc_progressor:message_data()].
convert_eventsink_event(Namespace, #{<<"process_id">> := ProcessID} = Event) ->
    encode(fun serialize_eventsink/3, Namespace, ProcessID, [Event]).

%% Internal functions

encode(Encoder, NS, ID, Events) ->
    [
        #{
            key => event_key(NS, ID),
            value => Encoder(NS, ID, Event)
        }
     || Event <- Events
    ].

event_key(NS, ID) ->
    <<NS/binary, " ", ID/binary>>.

%% eventsink serialization

serialize_eventsink(SourceNS, SourceID, Event) ->
    Codec = thrift_strict_binary_codec:new(),
    #{
        <<"event_id">> := EventID,
        <<"timestamp">> := DateTime,
        <<"payload">> := Payload
    } = Event,
    %% decode BYTEA to msgpack
    Content = erlang:binary_to_term(Payload),
    Metadata = maps:get(<<"metadata">>, Event, #{}),
    Timestamp = daytime_to_unixtime(DateTime),
    Data =
        {event, #mg_evsink_MachineEvent{
            source_ns = SourceNS,
            source_id = SourceID,
            event_id = EventID,
            created_at = serialize_timestamp(Timestamp),
            format_version = maps:get(<<"format_version">>, Metadata, undefined),
            data = Content
        }},
    Type = {struct, union, {mg_proto_event_sink_thrift, 'SinkEvent'}},
    case thrift_strict_binary_codec:write(Codec, Type, Data) of
        {ok, NewCodec} ->
            thrift_strict_binary_codec:close(NewCodec);
        {error, Reason} ->
            erlang:error({?MODULE, Reason})
    end.

daytime_to_unixtime({Date, {Hour, Minute, Second}}) when is_float(Second) ->
    daytime_to_unixtime({Date, {Hour, Minute, trunc(Second)}});
daytime_to_unixtime(Daytime) ->
    to_unixtime(calendar:datetime_to_gregorian_seconds(Daytime)).

to_unixtime(Time) when is_integer(Time) ->
    Time - ?EPOCH_DIFF.

serialize_timestamp(TimestampSec) ->
    Str = calendar:system_time_to_rfc3339(TimestampSec, [{unit, second}, {offset, "Z"}]),
    erlang:list_to_binary(Str).

%% lifecycle serialization

lifecycle_event(init) ->
    {machine_lifecycle_created, #{occurred_at => erlang:system_time(second)}};
lifecycle_event(repair) ->
    {machine_lifecycle_repaired, #{occurred_at => erlang:system_time(second)}};
lifecycle_event(remove) ->
    {machine_lifecycle_removed, #{occurred_at => erlang:system_time(second)}};
lifecycle_event({error, Reason}) ->
    {machine_lifecycle_failed, #{occurred_at => erlang:system_time(second), reason => Reason}}.

serialize_lifecycle(SourceNS, SourceID, Event) ->
    Codec = thrift_strict_binary_codec:new(),
    Data = serialize_lifecycle_event(SourceNS, SourceID, Event),
    Type = {struct, struct, {mg_proto_lifecycle_sink_thrift, 'LifecycleEvent'}},
    case thrift_strict_binary_codec:write(Codec, Type, Data) of
        {ok, NewCodec} ->
            thrift_strict_binary_codec:close(NewCodec);
        {error, Reason} ->
            erlang:error({?MODULE, Reason})
    end.

serialize_lifecycle_event(SourceNS, SourceID, {_, #{occurred_at := Timestamp}} = Event) ->
    #mg_lifesink_LifecycleEvent{
        machine_ns = SourceNS,
        machine_id = SourceID,
        created_at = serialize_timestamp(Timestamp),
        data = serialize_lifecycle_data(Event)
    }.

serialize_lifecycle_data({machine_lifecycle_created, _}) ->
    {machine, {created, #mg_lifesink_MachineLifecycleCreatedEvent{}}};
serialize_lifecycle_data({machine_lifecycle_failed, #{reason := Reason}}) ->
    {machine,
        {status_changed, #mg_lifesink_MachineLifecycleStatusChangedEvent{
            new_status =
                {failed, #mg_stateproc_MachineStatusFailed{
                    reason = Reason
                }}
        }}};
serialize_lifecycle_data({machine_lifecycle_repaired, _}) ->
    {machine,
        {status_changed, #mg_lifesink_MachineLifecycleStatusChangedEvent{
            new_status = {working, #mg_stateproc_MachineStatusWorking{}}
        }}};
serialize_lifecycle_data({machine_lifecycle_removed, _}) ->
    {machine, {removed, #mg_lifesink_MachineLifecycleRemovedEvent{}}}.
