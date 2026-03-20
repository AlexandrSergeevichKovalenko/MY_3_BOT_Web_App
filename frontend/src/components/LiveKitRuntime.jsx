import React from 'react';
import {
  LiveKitRoom,
  ControlBar,
  ConnectionStateToast,
  RoomAudioRenderer,
} from '@livekit/components-react';
import '@livekit/components-styles';

export default function LiveKitRuntime({
  serverUrl,
  token,
  connect = true,
  audio = true,
  video = false,
  onConnected,
  onDisconnected,
  onError,
  className = '',
  dataLkTheme = 'default',
  children,
}) {
  const content = typeof children === 'function'
    ? children({ ControlBar })
    : children;

  return (
    <LiveKitRoom
      serverUrl={serverUrl}
      token={token}
      connect={connect}
      audio={audio}
      video={video}
      onConnected={onConnected}
      onDisconnected={onDisconnected}
      onError={onError}
      className={className}
      data-lk-theme={dataLkTheme}
    >
      {content}
      <RoomAudioRenderer />
      <ConnectionStateToast />
    </LiveKitRoom>
  );
}
