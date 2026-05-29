import React, { useEffect } from 'react';
import { useYouTubeContext } from '../providers/YouTubeProvider.jsx';

export function YouTubeSection({ children }) {
  const context = useYouTubeContext();
  if (typeof context.render !== 'function') {
    throw new Error('YouTubeSection requires explicit render function');
  }

  useEffect(() => {
    console.info('youtube_section_mount', { provider_name: 'youtube' });
    return () => {
      console.info('youtube_section_unmount', { provider_name: 'youtube' });
    };
  }, []);

  return context.render({ children, context });
}

export default React.memo(YouTubeSection);
