%%%-------------------------------------------------------------------
%% @doc cdc_notifier public API
%% @end
%%%-------------------------------------------------------------------

-module(cdc_notifier_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    _ = application:ensure_all_started(cdc_progressor),
    cdc_notifier_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
