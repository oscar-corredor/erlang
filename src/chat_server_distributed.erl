-module(chat_server_distributed).

% -include_lib("eunit/include/eunit.hrl").

-export([chat_server_actor/4]).

% The server actor works like a small database and encapsulates all state of
% this simple implementation.
%
% * Users is a dictionary of user names to tuples of the form:
%     {user, Name, Subscriptions}
%   where Subscriptions is a set of channels that the user joined.
% * LoggedIn is a dictionary of the names of logged in users and their pid.
% * Channels is a dictionary of channel names to tuples:
%     {channel, Name, Messages}
%   where Messages is a list of messages, of the form:
%     {message, UserName, ChannelName, MessageText, SendTime}
chat_server_actor(Users, LoggedIn, Channels, ParentID) ->
    io:format("distributed chat server intialized"),
    receive
        {Sender, chat_log_in, Username, UserProcessId} -> 
            NewLoggedIn = dict:store(Username, UserProcessId, LoggedIn),
            chat_server_actor(Users,NewLoggedIn,Channels,ParentID);
        
        {Sender, join_channel, UserName, ChannelName} ->
            User = dict:fetch(UserName, Users), % assumes the user exists
            NewUser = join_channel(User, ChannelName),
            NewUsers = dict:store(UserName, NewUser, Users),
            Sender ! {self(), channel_joined},
            chat_server_actor(NewUsers, LoggedIn, Channels, ParentID);
            % TODO sync this info with the main server

        {Sender, send_message, UserName, ChannelName, MessageText, SendTime} ->
            Message = {message, UserName, ChannelName, MessageText, SendTime},
            % 1. Store message in its channel
            NewChannels = store_message(Message, Channels),
            % 2. Send logged in users the message, if they joined this channel
            broadcast_message_to_members(Users, LoggedIn, Message),
            Sender ! {self(), message_sent},
            chat_server_actor(Users, LoggedIn, NewChannels, ParentID);
            % TODO sync this info with the main server
        
        {Sender, get_channel_history, ChannelName} ->
            {channel, ChannelName, Messages} = find_or_create_channel(ChannelName, Channels),
            Sender ! {self(), channel_history, Messages},
            chat_server_actor(Users, LoggedIn, Channels,ParentID);

        {Sender, log_out, UserName} ->
            NewLoggedIn = dict:erase(UserName, LoggedIn),
            Sender ! {self(), logged_out},
            chat_server_actor(Users, NewLoggedIn, Channels,ParentID)
            % TODO sync this info with the main server
    end.


join_channel({user, Name, Subscriptions}, ChannelName) ->
    {user, Name, sets:add_element(ChannelName,Subscriptions)}.
    
store_message(Message, Channels) ->
    {message, _UserName, ChannelName, _MessageText, _SendTime} = Message,
    {channel, ChannelName, Messages} = find_or_create_channel(ChannelName, Channels),
    dict:store(ChannelName, {channel, ChannelName, Messages ++ [Message]}, Channels).

% Find channel, or create new channel
find_or_create_channel(ChannelName, Channels) ->
    case dict:find(ChannelName, Channels) of
        {ok, Channel} -> Channel;
        error ->         {channel, ChannelName, []}
    end.

% Broadcast `Message` to `Users` if they joined the channel and are logged in.
% (But don't send it to the sender.)
broadcast_message_to_members(Users, LoggedIn, Message) ->
    {message, SenderName, ChannelName, _MessageText, _SendTime} = Message,
    % For each LoggedIn user, fetch his subscriptions and check whether those
    % contain the channel
    Subscribed = fun(UserName, _) ->
        {user, _, Subscriptions} = dict:fetch(UserName, Users),
        IsMember = sets:is_element(ChannelName, Subscriptions),
        IsMember and (UserName /= SenderName)
    end,
    LoggedInAndSubscribed = dict:filter(Subscribed, LoggedIn),
    % Send messages
    dict:map(fun(_, UserPid) ->
        UserPid ! {self(), new_message, Message}
    end, LoggedInAndSubscribed),
    ok.