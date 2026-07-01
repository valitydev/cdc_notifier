-module(cdc_prg_utils).

-export([tables/1]).
-export([construct_table_name/2]).

-spec tables(atom()) -> map().
tables(NsId) ->
    #{
        processes => construct_table_name(NsId, "_processes"),
        tasks => construct_table_name(NsId, "_tasks"),
        schedule => construct_table_name(NsId, "_schedule"),
        running => construct_table_name(NsId, "_running"),
        events => construct_table_name(NsId, "_events")
    }.

-spec construct_table_name(atom(), string()) -> string().
construct_table_name(NsId, Postfix) ->
    "\"" ++ erlang:atom_to_list(NsId) ++ Postfix ++ "\"".
