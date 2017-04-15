-module(chat_server_distributed).

% -include_lib("eunit/include/eunit.hrl").

-export([chat_server_actor/3]).

% The server actor works like a small database and encapsulates all state of
% this simple implementation.
% * LoggedIn is a dictionary of the names of logged in users, 
%   their pid and their channels.{UserProcessId, Subscriptions}
%   where Subscriptions is a set of channels that the user joined.
% * Channels is a dictionary of channel names to tuples:
%     {channel, Name, Messages}
%   where Messages is a list of messages, of the form:
%     {message, UserName, ChannelName, MessageText, SendTime}

chat_server_actor(LoggedIn, Channels, ParentID) ->    
    receive
        {_, chat_log_in, Username, UserProcessId, Subscriptions} -> 
            NewLoggedIn = dict:store(Username, {UserProcessId, Subscriptions}, LoggedIn),
            chat_server_actor(NewLoggedIn,Channels,ParentID);
        
        {Sender, join_channel, Username, ChannelName} ->
            User = dict:fetch(Username, LoggedIn), % assumes the user exists
            NewUser = join_channel(User, ChannelName),
            NewLoggedIn = dict:store(Username, NewUser, LoggedIn),
            Sender ! {self(), channel_joined},
            ParentID ! {self(), joined_channel, Username, ChannelName},
            chat_server_actor(NewLoggedIn, Channels, ParentID);
            
        % for when a user sends a message
        {Sender, send_message, Username, ChannelName, MessageText, SendTime} ->
            
            Message = {message, Username, ChannelName, MessageText, SendTime},
            % 1. Store message in its channel
            NewChannels = store_message(Message, Channels),
            % 2. Send logged in users the message, if they joined this channel            
            Sender ! {self(), message_sent},
            broadcast_message_to_members(LoggedIn, Message),
            ParentID ! {self(), sent_message, {message, Username, ChannelName, MessageText, SendTime}},
            
            chat_server_actor(LoggedIn, NewChannels, ParentID);
        
        % FOR WHEN A MESSAGE WAS SENT IN ANOTHER CHAT SERVER
        {_, new_message, Username, ChannelName, MessageText, SendTime} ->            
            Message = {message, Username, ChannelName, MessageText, SendTime},
            % 1. Store message in its channel
            NewChannels = store_message(Message, Channels),
            % 2. Send logged in users the message if they joined this channel
            broadcast_message_to_members(LoggedIn, Message),            
            chat_server_actor(LoggedIn, NewChannels, ParentID);
            
        
        {Sender, get_channel_history, ChannelName} ->
            {channel, ChannelName, Messages} = find_or_create_channel(ChannelName, Channels),
            Sender ! {self(), channel_history, Messages},
            chat_server_actor(LoggedIn, Channels,ParentID);

        {Sender, log_out, Username} ->
            NewLoggedIn = dict:erase(Username, LoggedIn),
            Sender ! {self(), logged_out},
            % notify the main server about the log out
            ParentID ! {self(),logged_out, Username},
            chat_server_actor(NewLoggedIn, Channels,ParentID)           
    end.


join_channel({UserProcessId, Subscriptions}, ChannelName) ->
    {UserProcessId, sets:add_element(ChannelName,Subscriptions)}.
    
    
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
% {UserProcessId, Subscriptions}
broadcast_message_to_members(LoggedIn, Message) ->
    {message, SenderName, ChannelName, _MessageText, _SendTime} = Message,
    % For each LoggedIn user, fetch his subscriptions and check whether those
    % contain the channel
    Subscribed = fun(UserName, {_, Subscriptions}) ->        
        IsMember = sets:is_element(ChannelName, Subscriptions),
        IsMember and (UserName /= SenderName)
    end,
    LoggedInAndSubscribed = dict:filter(Subscribed, LoggedIn),
    % Send messages
    dict:map(fun(_, {UserProcessId, _}) ->
        UserProcessId ! {self(), new_message, Message}
    end, LoggedInAndSubscribed),
    ok.