%%%-------------------------------------------------------------------
%% @doc cdc_progressor top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(cdc_progressor_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% sup_flags() = #{strategy => strategy(),         % optional
%%                 intensity => non_neg_integer(), % optional
%%                 period => pos_integer()}        % optional
%% child_spec() = #{id => child_id(),       % mandatory
%%                  start => mfargs(),      % mandatory
%%                  restart => restart(),   % optional
%%                  shutdown => shutdown(), % optional
%%                  type => worker(),       % optional
%%                  modules => modules()}   % optional
init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 0,
        period => 1
    },
    ChildSpecs = specs(),
    {ok, {SupFlags, ChildSpecs}}.

%% internal functions

specs() ->
    {ok, Databases} = application:get_env(epg_connector, databases),
    {ok, StreamsConfig} = application:get_env(cdc_progressor, streams),
    maps:fold(
        fun(DbRef, SlotsMap, Acc) ->
            DbOpts = maps:get(DbRef, Databases),
            spec_for_slots(DbOpts, SlotsMap, Acc)
        end,
        [],
        StreamsConfig
    ).

spec_for_slots(DbOpts, SlotsMap, Init) ->
    maps:fold(
        fun(ReplSlotName, Streams, Acc) ->
            Spec = cdc_progressor:child_spec(DbOpts, ReplSlotName, Streams),
            [Spec | Acc]
        end,
        Init,
        SlotsMap
    ).
