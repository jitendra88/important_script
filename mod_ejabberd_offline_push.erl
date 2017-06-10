%% name of module must match file name
-module(mod_ejabberd_offline_push).
-author("dev@codepond.org").


-export([start/2, stop/1, store_packet/1]).

-include("/usr/local/lib/ejabberd-17.04/include/ejabberd.hrl").
-include("/home/ejabberd_stuff/ejjaberd_modules/xmpp/include/xmpp.hrl").


start(_Host, _Opt) ->
  inets:start(),
  ejabberd_hooks:add(offline_message_hook, _Host, ?MODULE, store_packet, 50).

stop(_Host) ->
  ejabberd_hooks:delete(offline_message_hook, _Host, ?MODULE, store_packet, 50).


store_packet({_Action, #message{from = From, to = To} = Packet} = Acc) ->
  case Packet#message.body /= [] of
    true ->
      [{text, _, Body}] = Packet#message.body,
      post_offline_message(From, To, Body, Packet#message.id)
  end.


%%store_packet(Packet)
%%  when (Packet#message.type == chat) and (Packet#message.body /= []) ->
%%  [{text, _, Body}] = Packet#message.body,
%%  post_offline_message(Packet#message.id, Packet#message.id, Body, "SubType", Packet#message.id);
%%store_packet(_Packet) ->
%%  ok.

post_offline_message(From, To, Body, MsgId) ->
  ToUser = binary_to_list(To#jid.luser),
  FromUser = binary_to_list(From#jid.luser),
  FinalBody = binary_to_list(Body),
  MessageId = binary_to_list(MsgId),
  DataBody = "{\"toJID\":\"" ++ ToUser ++ "\",\"fromJID\":\"" ++ FromUser ++ "\",\"body\":\"" ++ FinalBody ++ "\",\"messageID\":\"" ++ MessageId ++ "\"}",
  Method = post,
  URL = "http://qa-api.myu.co/midl/api/v3/pushnotification/chat?debug=true",
  Header = [],
  Type = "application/json",
  HTTPOptions = [],
  Options = [],
  %%  inets:start(),
  %%  ssl:start(),
  R = httpc:request(Method, {URL, Header, Type, DataBody}, HTTPOptions, Options),
  io:fwrite(R).

