%%%-------------------------------------------------------------------
%%% @author Chen Slepher <slepher@issac.local>
%%% @copyright (C) 2016, Chen Slepher
%%% @doc
%%%
%%% @end
%%% Created : 22 Apr 2016 by Chen Slepher <slepher@issac.local>
%%%-------------------------------------------------------------------
-module(mysql_util).

%% API
-export([query_statement/2, query_statement/3, update_statement/3, delete_statement/2]).

%%%===================================================================
%%% API
%%%===================================================================

query_statement(Tab, Attrs) ->
    query_statement(Tab, Attrs, #{}).
    
query_statement(Tab, Attrs, Columns) when is_list(Columns) ->
    query_statement(Tab, Attrs, #{columns => Columns});
query_statement(Tab, Attrs, Options) when is_map(Options) ->
    Columns = maps:get(columns, Options, undefined),
    ColumnStr = 
        case Columns of
            undefined ->
                "*";
            _ ->
                string:join(
                  lists:map(
                    fun(Column) ->
                            column(Column)
                    end, Columns), ", ")
        end,
    ConditionStr = 
        case Attrs of
            [] ->
                undefined;
            _ ->
                ["where ", string:join(generate_attr_block(Attrs, Options), " and ")]
        end,
    Groups = maps:get(group_by, Options, undefined),
    GroupStr = 
        case Groups of
            undefined ->
                undefined;
            _ ->
                ["group by ", string:join(lists:map(fun to_list/1, Groups), ", ")]
        end,
    list_to_binary(string:join(
                     filter_empty(["select", ColumnStr, "from",
                                   to_list(Tab), ConditionStr, GroupStr]), " ")).
    
    
update_statement(Tab, Type, Attrs) ->
    SetBlock = generate_attr_block(Attrs, #{}),
    Head = 
        case Type of
            insert ->
                "insert into";
            insert_ignore ->
                "insert ignore into";
            replace ->
                "replace into";
            update ->
                "update";
            delete ->
                "delete from"
        end,
    list_to_binary(
      lists:flatten([Head, " ", 
                     to_list(Tab), " set ", string:join(SetBlock, ", ")])).

delete_statement(Tab, Attrs) ->
    ConditionStr = 
        case Attrs of
            [] ->
                undefined;
            _ ->
                ["where ", string:join(generate_attr_block(Attrs, #{}), " and ")]
        end,
    list_to_binary(string:join(
                     filter_empty(["delete from",
                                   to_list(Tab), ConditionStr]), " ")).

format_value(Value) when is_binary(Value) ->
    io_lib:format("'~s'", [Value]);
format_value(Value) ->
    io_lib:format("~p", [Value]).

generate_attr_block(Attrs, #{direct := true}) ->
    lists:map(
      fun({Key, [undefined, V2]}) ->
              io_lib:format("~p <= ~s", [Key, format_value(V2)]);
         ({Key, [V1, undefined]}) ->
              io_lib:format("~p >= ~s", [Key, format_value(V1)]);
         ({Key, [V1, V2]}) ->
              io_lib:format("~p between ~s and ~s", [Key, format_value(V1), format_value(V2)]);
         ({Key, Value}) ->
              io_lib:format("~p = ~s", [Key, format_value(Value)])
       end, Attrs);
generate_attr_block(Attrs, #{}) ->
    lists:map(
      fun({Key, [undefined, _V2]}) ->
              io_lib:format("~p <= ?", [Key]);
         ({Key, [_V1, undefined]}) ->
              io_lib:format("~p >= ?", [Key]);
         ({Key, [_V1, _V2]}) ->
              io_lib:format("~p between ? and ?", [Key]);
         ({Key, _Value}) ->
              io_lib:format("~p = ?", [Key]);
         (Key) when is_atom(Key) ->
              io_lib:format("~p = ?", [Key]);
         (Key) ->
              io_lib:format("~s = ?", [Key])
      end, Attrs).

to_list(Atom) when is_atom(Atom) ->
    atom_to_list(Atom);
to_list(Binary) when is_binary(Binary)  ->
    binary_to_list(Binary);
to_list(List) when is_list(List) ->
    List.

filter_empty(Strings) ->
    lists:filter(
      fun(undefined) ->
              false;
         ("") ->
              false;
         (_) ->
              true
      end, Strings).


%%--------------------------------------------------------------------
%% @doc
%% @spec
%% @end
%%--------------------------------------------------------------------

%%%===================================================================
%%% Internal functions
%%%===================================================================
column({ColumnDesc, Rename}) ->
    Column = format_desc(ColumnDesc, Rename),
    [Column, " as ", to_list(Rename)];
column(Column) ->
    to_list(Column).

format_desc({Fun, Column}, _Rename) ->
    [to_list(Fun), "(", to_list(Column), ")"];
format_desc({Fun}, Rename) ->
    format_desc({Fun, Rename}, Rename);
format_desc(Column, _Rename) ->
    to_list(Column).
