-module(cdc_prg_ct_helper).

-include("cdc_prg_ct.hrl").

-export([start_app/1]).
-export([start_app/2]).
-export([stop_app/1]).
-export([create_kafka_topics/0]).
-export([delete_kafka_topics/0]).

-define(TOPIC_CONFIG(Topic), #{
    configs => [],
    num_partitions => 1,
    assignments => [],
    replication_factor => 1,
    name => Topic
}).

-spec start_app(Application :: atom()) -> ok | no_return().
start_app(brod = Application) ->
    start_app(Application, ?DEFAULT_BROD_CONF);
start_app(epg_connector = Application) ->
    start_app(Application, ?DEFAULT_EPG_CONF);
start_app(progressor = Application) ->
    start_app(Application, ?DEFAULT_PRG_CONF);
start_app(cdc_progressor = Application) ->
    start_app(Application, ?DEFAULT_CDC_CONF).

-spec start_app(Application :: atom(), Env :: [{Par :: atom(), Val :: term()}]) -> ok | no_return().
start_app(Application, Env) ->
    ok = stop_app(Application),
    ok = load_app(Application),
    ok = application:set_env([{Application, Env}]),
    {ok, _} = application:ensure_all_started(Application),
    ok.

-spec stop_app(Application :: atom()) -> ok | no_return().
stop_app(Application) ->
    maybe
        ok ?= application:stop(Application),
        ok ?= application:unload(Application)
    else
        {error, {not_started, _}} ->
            _ = application:unload(Application),
            ok;
        {error, {not_loaded, _}} ->
            ok
    end.

-spec create_kafka_topics() -> ok | no_return().
create_kafka_topics() ->
    create_kafka_topics([
        ?PRG_EVENTSINK_TOPIC,
        ?PRG_LIFECYCLE_TOPIC,
        ?CDC_EVENTSINK_TOPIC,
        ?CDC_LIFECYCLE_TOPIC
    ]).

-spec create_kafka_topics([Topic :: binary()]) -> ok | no_return().
create_kafka_topics(Topics) ->
    TopicConfig = lists:foldl(
        fun(Topic, Acc) ->
            [?TOPIC_CONFIG(Topic) | Acc]
        end,
        [],
        Topics
    ),
    ok = brod:create_topics(?BROKERS, TopicConfig, #{timeout => 5000}).

-spec delete_kafka_topics() -> ok | no_return().
delete_kafka_topics() ->
    delete_kafka_topics([
        ?PRG_EVENTSINK_TOPIC,
        ?PRG_LIFECYCLE_TOPIC,
        ?CDC_EVENTSINK_TOPIC,
        ?CDC_LIFECYCLE_TOPIC
    ]).

-spec delete_kafka_topics([Topic :: binary()]) -> ok | no_return().
delete_kafka_topics(Topics) ->
    ok = brod:delete_topics(?BROKERS, Topics, 5000).

%% Internal functions
load_app(AppName) ->
    maybe
        ok ?= application:load(AppName)
    else
        {error, {already_loaded, _}} ->
            ok
    end.
