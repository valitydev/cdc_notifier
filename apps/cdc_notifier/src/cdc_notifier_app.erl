%%%-------------------------------------------------------------------
%% @doc cdc_notifier public API
%% @end
%%%-------------------------------------------------------------------

-module(cdc_notifier_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    _ = start_listener(),
    _ = application:ensure_all_started(cdc_progressor),
    cdc_notifier_sup:start_link().

stop(_State) ->
    ok.

%% internal functions

start_listener() ->
    Port = application:get_env(cdc_notifier, port, 8022),
    Routes = cowboy_router:compile([
        {'_', [
          {"/metrics/[:registry]", prometheus_cowboy2_handler, []}
        ]}
    ]),
    {ok, _} = cowboy:start_clear(http_listener, [{port, Port}], #{env => #{dispatch => Routes}}).
