%%%-------------------------------------------------------------------
%% @doc cdc_progressor public API
%% @end
%%%-------------------------------------------------------------------

-module(cdc_progressor_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    cdc_progressor_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
