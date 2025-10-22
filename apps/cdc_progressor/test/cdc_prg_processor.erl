-module(cdc_prg_processor).

-export([process/3]).

-spec process(_Req, _Opts, _Ctx) -> _.
process(_Req, _Opts, _Ctx) ->
    {ok, #{events => []}}.
