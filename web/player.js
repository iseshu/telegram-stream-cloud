(() => {
  'use strict';
  const video = document.getElementById('video');
  const link = document.getElementById('link');
  const status = document.getElementById('status');
  try {
    new Plyr(video, {
      title: document.querySelector('h1').textContent,
      iconUrl: '/assets/vendor/jplayer/plyr.svg',
      blankVideo: '',
      controls: ['play-large', 'play', 'progress', 'current-time', 'duration', 'mute', 'volume', 'captions', 'settings', 'pip', 'airplay', 'fullscreen', 'imax'],
      keyboard: { focused: true, global: true },
      seekTime: 10,
      settings: ['captions', 'speed'],
      storage: { enabled: false }
    });
  } catch (_) {
    status.textContent = 'Using browser controls. You can also open the stream in VLC.';
    video.controls = true;
  }
  // Attach the source after construction to use native HTTP range playback.
  // The pinned JPlayer MKV auto-loader only appends an initialization segment.
  video.src = video.dataset.stream;
  video.load();
  video.addEventListener('error', () => {
    status.textContent = 'This video could not be played. Try VLC or download the file; your browser may not support its format.';
  });
  document.getElementById('copy').addEventListener('click', async () => {
    try {
      await navigator.clipboard.writeText(link.value);
      status.textContent = 'Stream link copied.';
    } catch (_) {
      link.focus();
      link.select();
      status.textContent = 'Copy the selected stream link.';
    }
  });
  document.getElementById('vlc').addEventListener('click', () => {
    window.location.href = 'vlc://' + link.value;
  });
})();
