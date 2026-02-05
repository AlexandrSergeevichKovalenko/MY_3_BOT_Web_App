import React, { useEffect, useMemo, useRef, useState } from 'react';
import {
  LiveKitRoom,
  ControlBar,
  ConnectionStateToast,
  RoomAudioRenderer,
} from '@livekit/components-react';
import '@livekit/components-styles';
import './App.css';
import * as echarts from 'echarts';

// URL вашего сервера LiveKit
const livekitUrl = "wss://implemrntingvoicetobot-vhsnc86g.livekit.cloud";

class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error) {
    return { hasError: true, error };
  }

  componentDidCatch(error, info) {
    console.error('WebApp crashed:', error, info);
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="webapp-page">
          <div className="webapp-card">
            <header className="webapp-header">
              <span className="pill">Telegram Web App</span>
              <h1>Ошибка загрузки</h1>
              <p>Произошла ошибка при запуске приложения. Попробуйте перезагрузить.</p>
            </header>
            <div className="webapp-error">
              {this.state.error?.message || 'Неизвестная ошибка'}
            </div>
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}

function AppInner() {
  const telegramApp = useMemo(() => window.Telegram?.WebApp, []);
  const isWebAppMode = useMemo(() => {
    const params = new URLSearchParams(window.location.search);
    const isWebappPath =
      window.location.pathname === '/webapp' ||
      window.location.pathname === '/webapp/review';
    return Boolean(telegramApp?.initData) || params.get('mode') === 'webapp' || isWebappPath;
  }, [telegramApp]);

  const [initData, setInitData] = useState(telegramApp?.initData || '');
  const [sessionId, setSessionId] = useState(null);
  const [webappUser, setWebappUser] = useState(null);
  const [results, setResults] = useState([]);
  const [sentences, setSentences] = useState([]);
  const [webappError, setWebappError] = useState('');
  const [webappLoading, setWebappLoading] = useState(false);
  const [translationDrafts, setTranslationDrafts] = useState({});
  const [finishMessage, setFinishMessage] = useState('');
  const [dictionaryWord, setDictionaryWord] = useState('');
  const [dictionaryResult, setDictionaryResult] = useState(null);
  const [dictionaryError, setDictionaryError] = useState('');
  const [dictionaryLoading, setDictionaryLoading] = useState(false);
  const [dictionarySaved, setDictionarySaved] = useState('');
  const [dictionaryDirection, setDictionaryDirection] = useState('ru-de');
  const [collocationsVisible, setCollocationsVisible] = useState(false);
  const [collocationsLoading, setCollocationsLoading] = useState(false);
  const [collocationsError, setCollocationsError] = useState('');
  const [collocationOptions, setCollocationOptions] = useState([]);
  const [selectedCollocation, setSelectedCollocation] = useState(null);
  const [flashcardExitSummary, setFlashcardExitSummary] = useState(false);
  const [exportLoading, setExportLoading] = useState(false);
  const [youtubeInput, setYoutubeInput] = useState('');
  const [youtubeId, setYoutubeId] = useState('');
  const [youtubeError, setYoutubeError] = useState('');
  const [videoExpanded, setVideoExpanded] = useState(false);
  const [youtubeTranscript, setYoutubeTranscript] = useState([]);
  const [youtubeTranscriptError, setYoutubeTranscriptError] = useState('');
  const [youtubeTranscriptLoading, setYoutubeTranscriptLoading] = useState(false);
  const [manualTranscript, setManualTranscript] = useState('');
  const [selectionText, setSelectionText] = useState('');
  const [selectionPos, setSelectionPos] = useState(null);
  const [lastLookupScrollY, setLastLookupScrollY] = useState(null);
  const [historyItems, setHistoryItems] = useState([]);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyError, setHistoryError] = useState('');
  const [historyVisible, setHistoryVisible] = useState(false);
  const [finishStatus, setFinishStatus] = useState('idle');
  const [explanations, setExplanations] = useState({});
  const [explanationLoading, setExplanationLoading] = useState({});
  const [flashcardsVisible, setFlashcardsVisible] = useState(false);
  const [flashcardsOnly, setFlashcardsOnly] = useState(false);
  const [flashcardsLoading, setFlashcardsLoading] = useState(false);
  const [flashcardsError, setFlashcardsError] = useState('');
  const [flashcards, setFlashcards] = useState([]);
  const [flashcardPool, setFlashcardPool] = useState([]);
  const [flashcardIndex, setFlashcardIndex] = useState(0);
  const [flashcardSelection, setFlashcardSelection] = useState(null);
  const [flashcardOptions, setFlashcardOptions] = useState([]);
  const [flashcardSetSize, setFlashcardSetSize] = useState(15);
  const [flashcardDurationSec, setFlashcardDurationSec] = useState(10);
  const [flashcardTimerDurationSec, setFlashcardTimerDurationSec] = useState(10);
  const [flashcardSessionActive, setFlashcardSessionActive] = useState(false);
  const [flashcardTimerKey, setFlashcardTimerKey] = useState(0);
  const [topics, setTopics] = useState([
    '💼 Business',
    '🏥 Medicine',
    '🎨 Hobbies',
    '✈️ Travel',
    '🔬 Science',
    '💻 Technology',
    '🖼️ Art',
    '🎓 Education',
    '🍽️ Food',
    '⚽ Sports',
    '🌿 Nature',
    '🎵 Music',
    '📚 Literature',
    '🧠 Psychology',
    '🏛️ History',
    '📰 News',
  ]);
  const [topicsLoading, setTopicsLoading] = useState(false);
  const [topicsError, setTopicsError] = useState('');
  const [selectedTopic, setSelectedTopic] = useState('💼 Business');
  const [selectedSections, setSelectedSections] = useState(new Set());
  const [flashcardSetComplete, setFlashcardSetComplete] = useState(false);
  const [flashcardStats, setFlashcardStats] = useState({ total: 0, correct: 0, wrong: 0 });
  const [flashcardTimedOut, setFlashcardTimedOut] = useState(false);
  const [folders, setFolders] = useState([]);
  const [foldersLoading, setFoldersLoading] = useState(false);
  const [foldersError, setFoldersError] = useState('');
  const [dictionaryFolderId, setDictionaryFolderId] = useState('none');
  const [flashcardFolderMode, setFlashcardFolderMode] = useState('all');
  const [flashcardFolderId, setFlashcardFolderId] = useState('');
  const [showNewFolderForm, setShowNewFolderForm] = useState(false);
  const [newFolderName, setNewFolderName] = useState('');
  const [newFolderColor, setNewFolderColor] = useState('#5ddcff');
  const [newFolderIcon, setNewFolderIcon] = useState('book');
  const [userAvatar, setUserAvatar] = useState('');
  const [menuOpen, setMenuOpen] = useState(false);
  const [menuMultiSelect, setMenuMultiSelect] = useState(true);
  const [analyticsPeriod, setAnalyticsPeriod] = useState('week');
  const [analyticsLoading, setAnalyticsLoading] = useState(false);
  const [analyticsError, setAnalyticsError] = useState('');
  const [analyticsSummary, setAnalyticsSummary] = useState(null);
  const [analyticsPoints, setAnalyticsPoints] = useState([]);
  const [analyticsCompare, setAnalyticsCompare] = useState([]);
  const [analyticsRank, setAnalyticsRank] = useState(null);

  const dictionaryRef = useRef(null);
  const flashcardsRef = useRef(null);
  const translationsRef = useRef(null);
  const youtubeRef = useRef(null);
  const autoAdvanceTimeoutRef = useRef(null);
  const revealTimeoutRef = useRef(null);
  const flashcardIndexRef = useRef(0);
  const flashcardSelectionRef = useRef(null);
  const audioContextRef = useRef(null);
  const positiveAudioRef = useRef(null);
  const negativeAudioRef = useRef(null);
  const avatarInputRef = useRef(null);
  const analyticsRef = useRef(null);
  const analyticsTrendRef = useRef(null);
  const analyticsCompareRef = useRef(null);

  const safeStorageGet = (key) => {
    try {
      return window.localStorage.getItem(key);
    } catch (error) {
      console.warn('localStorage unavailable', error);
      return null;
    }
  };

  const safeStorageSet = (key, value) => {
    try {
      window.localStorage.setItem(key, value);
    } catch (error) {
      console.warn('localStorage unavailable', error);
    }
  };

  const safeStorageRemove = (key) => {
    try {
      window.localStorage.removeItem(key);
    } catch (error) {
      console.warn('localStorage unavailable', error);
    }
  };

  const POSITIVE_BEEP = 'data:audio/wav;base64,UklGRnwpAABXQVZFZm10IBAAAAABAAEARKwAAIhYAQACABAAZGF0YVgpAAAAAAAI4Q+CF8QeiyW6Kzox9DXWOc880z7bP+I/6T7zPAc6MzaFMQ8s6SUqH+4XUhB0CHQAdPiQ8Ovoo+HU2pvUEc9Lyl3GVsNEwS3AF8ACwerCyMWPyTLOnNO52XDgpuc97xj3F/8YB/4OqBb2Hc0kDyukMHY1cDmEPKU+yj/vPxI/OD1oOq42FzK3LKQm9R/GGDMRWwleAVz5c/HF6XHik9tI1ajPy8rDxqLDc8FAwA3A28CmwmjFFsmhzfXS/9im38/mXe4x9i7+MAYbDs0VJx0NJGEqCzD0NAg5Nzx0PrY/9z84P3s9xjolN6cyXS1dJ78gnBkTEkIKRwJE+lfyoepB41Tc99VC0E7LLcfxw6bBVsAGwLbAZcIMxaDIEs1R0kfY3d755X3tS/VE/UgFNw3xFFccSyOxKXAvbzSdOOY7Pz6eP/0/Wz+6PSE7mjc0MwEuFCiGIXIa8xIoCzADLfs7833rEuQX3ajW39DUy5rHQ8TcwW/AAsCVwCjCs8QsyIbMrtGR1xfeJOWe7GX0W/xfBFIMFBSFG4ci/yjSLugzLjiSOwc+gz//P3o/9j15Ow04vzOiLskoTCJGG9ETDgwZBBX8IPRb7OXk291b137RXMwKyJnEFsKMwAHAeMDtwV3Eu8f9yw/R3dZR3VHkwOuA83L7dgNtCzYTsRrCIUsoMS5eM703PDvMPWQ//j+WPy8+zTt8OEc0QC98KRAjGByvFPIMAgX+/Ab1Ou255aLeENgg0ujMfcjxxFPCrMAEwF3AtsEJxE7Hdstx0CzWjtx/4+Pqm/KK+o0ChwpXEt0Z+yCUJ48t0jJJN+I6jj1DP/k/rz9kPh886DjMNN0vLCrTI+kcixXXDesF6P3s9RnujuZq38jYxNJ1zfLITMWSws/ACsBGwILBucPjxvLK1s981c3br+IH6rfxofmkAaAJdxEGGTIg3CbpLEMy0jaFOkw9Hj/xP8Q/lz5tPFE5TzV2MNsqkyS5HWYWug7TBtH+0/b67mXnM+CB2WrTBs5ryavF1cL2wBTAMsBSwW3De8Zxyj7Pz9QN2+DhLOnU8Ln4ugC5CJUQLxhnHyEmQiyxMVg2JToIPfY+5j/WP8Y+uTy4Oc81DTGHK1Ilhh5AF50Puwe6/7r32+896P/gPdoT1JnO5skMxhzDIMEgwCLAJMEjwxbG8smoziTUUNoT4VPo8u/R99L/0ge0D1YXmx5lJZgrHDHbNcI5wDzKPtg/5T/xPgE9GzpMNqIxMSwOJlMfGRh/EKIIowCi+L7wF+nM4fravtQvz2TKccZlw03BMcAVwPrA3MK0xXfJFc5705TZSOB75xDv6vbo/uoG0Q58Fs0dpiTsKoYwXDVcOXU8mz7GP/E/Gj9GPXs6xjY0MtksySYeIPEYYBGJCYwBivmh8fHpmuK622vVx8/lytjGssN9wUTAC8DTwJnCVsX+yITN1NLa2H7fpOYw7gP2//0CBu0NoRX+HOYjPirsL9o08zgnPGk+sT/5Pz8/iD3ZOj03xDJ+LYIn5yDHGUAScAp2AnL6hfLN6mrje9wa1mLQactDxwHEscFbwATAr8BZwvrEiMj2zDDSI9i13s7lUO0d9Rb9GQUJDcUULRwkI44pUC9VNIc41js0Ppk//T9hP8Y9MzuyN1AzIS44KK4hnBofE1YLXwNb+2nzqus85D7dzNb/0O/LsMdUxOjBdcABwI/AHMKhxBXIasyO0W3X79365HHsN/Qt/DAEJAzoE1sbYCLbKLIuzTMYOIE7/D19P/8/gD8BPoo7IzjbM8Iu7Sh0InAb/hM7DEgERPxO9IfsD+UD3n/XntF4zCHIqsQiwpLAAsBywOLBTMSlx+HL79C61irdJ+SU61LzRPtHAz8LCROHGpohJigRLkIzpjcqO8A9Xj/9P5s/Oj7eO5I4YjRgL58pNyNCHNsUIA0xBS39NPVm7ePlyd412EDSBM2UyAPFX8KzwAXAWMCrwfnDOMdby1LQCNZn3Fbjt+pu8lv6XgJZCioSshnTIG8nbi21MjE3zzqBPTw/+D+zP28+Lzz9OOc0/C9PKvkjEx23FQQOGQYW/hr2Ru655pLf7djl0pLNCslfxaDC18AMwELAeMGqw87G2Mq4z1nVptuG4tvpivFz+XUBcglKEdsYCSC3JsgsJjK6NnI6Pz0WP/A/yD+gPn08ZjlpNZUw/Sq5JOIdkhboDgEHAP8B9yfvkOdc4KfZjNMjzoPJvsXjwv7AFsAvwEjBXsNnxlfKIM+s1Ofat+EB6afwi/iMAIsIaBAEGD4f/CUgLJMxPzYROvo87T7kP9k/zz7HPMw56DUrMakreCWvHmwXyg/pB+n/6fcJ8GnoKOFj2jXUt87/ySDGKsMowSPAH8AbwRXDAsbayYrOAtQq2urgKOjF76P3o/+kB4YPKxdyHj8ldiv+MMI1rjmxPME+1T/oP/o+Dz0vOmQ2wDFTLDQmfB9FGKwQ0QjSAND46/BC6fXhINvg1E3PfsqFxnTDVsE0wBPA8sDPwqHFX8n3zVnTb9kf4FDn4+689rr+vAakDlEWpB2AJMoqZzBCNUc5ZjySPsI/8j8iP1M9jjreNlEy+izuJkYgHBmNEbcJuwG4+c7xHerE4uDbjtXmz//K7cbBw4fBSMAJwMzAjMJDxebIZ82z0rXYVt955gPu1fXQ/dMFwA11FdQcvyMbKs0vvzTdOBc8Xz6sP/o/Rj+UPes6VTfgMp8tpicPIfIZbRKeCqQCofqy8vnqlOOi3D3WgdCDy1nHEsS8wWDABMCpwEzC6MRxyNrMENL+147epOUj7e/05/zrBNwMmRQDHP0iaikxLzo0cTjFOyk+kz/+P2g/0j1EO8g3bDNBLl0o1iHHGkwThAuNA4r7l/PW62bkZd3v1h7RCszHx2XE88F6wAHAicAQwpDE/8dPzG7RSdfI3dDkRewJ9P77AgT3C7sTMBs4Ircoki6xMwE4cDvwPXc//z+GPw0+mzs5OPYz4i4RKZsimhsqFGkMdgRz/Hz0tOw55Sreo9e/0ZTMOMi8xC7CmMACwG3A18E7xI/HxsvP0JbWA93942frJPMV+xkDEQvdElwaciECKPAtJjOPNxg7tD1XP/w/oD9EPu47pzh9NH8vwyleI2wcBxVODV8FXP1i9ZPtDubx3lrYYdIgzavIFcVswrrABsBUwKHB6cMjx0HLM9Dl1UHcLOOL6kDyLfowAisK/RGHGaogSydNLZkyGje9OnQ9ND/3P7g/eT4/PBM5ATUaMHIqICQ8HeMVMg5IBkX+SfZz7uTmut8S2QbTr80iyXLFrcLewA7APsBuwZrDuca+ypnPNtWA21zir+lc8UT5RgFECR0RsBjhH5EmpywJMqE2XzoxPQ4/7T/MP6o+jDx7OYI1szAgK+AkCx6+FhUPMAcu/y/3VO+754TgzNmt00DOnMnRxfHCBsEYwCvAP8FPw1LGPsoCz4rUwdqO4dXoevBc+F0AXQg7ENgXFh/WJf4rdjEmNv456zzlPuE/3T/YPtY84DkBNkkxyyudJdgelxf3DxcIFwAX+DbwlOhR4YjaV9TVzhjKNMY5wzHBJ8AcwBPBBsPvxcHJbc7g0wTawuD855jvdfd0/3UHWQ//FkkeGSVUK+AwqTWZOaI8uD7RP+o/Aj8dPUI6fTbdMXQsWSakH3AY2RD/CAAB//gY8W7pHuJH2wPVa8+XyprGg8NgwTjAEMDqwMHCjsVGydrNONNJ2fffJee27o72i/6NBnYOJRZ6HVokpypIMCg1MjlWPIg+vj/0Pyk/YD2hOvY2bjIbLRMnbiBHGboR5gnqAef5/PFJ6u3iB9yx1QTQGcsDx9HDkcFNwAjAxMB/wjHFz8hLzZLSkdgt307m1u2n9aL9pQWSDUkVqhyZI/gpri+lNMg4BzxVPqg/+z9NP6E9/TpsN/wywC3LJzchHRqaEswK0wLP+uDyJeu+48ncYdag0J7LbscixMbBZcADwKLAQMLWxFrIvszv0drXZt555ffswfS5/LwErgxsFNkb1iJGKREvHzRbOLQ7Hj6OP/4/bj/ePVY73zeIM2IugSj9IfEaeROyC7wDuPvF8wLskOSM3RPXPtElzN3HdsT/wYDAAcCDwATCf8ToxzPMTtEl16DdpeQY7Nzz0PvTA8kLjxMGGxEikyhyLpYz6zdfO+Q9cT//P4s/GD6sO1A4ETQBLzQpwiLEG1YUlwylBKH8qvTh7GTlUt7I19/RsMxOyM3EOsKfwAPAZ8DMwSrEecery7DQctbc3NPjO+v38uf66gLjCrASMhpLId0n0C0KM3g3BjunPVE//D+lP08+/zu9OJc0ni/mKYUjlhwzFXsNjgWK/ZD1wO055hnfftiC0jzNw8gnxXjCwcAHwE/Al8HZww3HJssU0MLVGtwC41/qE/L++QEC/QnQEVwZgiAmJywtfDICN6o6Zz0tP/U/vD+DPk48KDkbNTkwlSpGJGYdDxZfDnYGdP539qDuD+fi3zfZJ9PMzTrJhcW6wubAD8A6wGXBi8OkxqTKes8U1VrbM+KE6S/xFvkYARYJ8BCFGLgfbCaFLOsxiTZMOiQ9Bj/rP88/sz6bPI85nDXRMEIrBiU0HukWQg9eB13/XveB7+fnreDy2c/TXs60yeXF/8IPwRvAKMA2wUDDPsYlyuTOaNSb2mXhquhM8C74LgAvCA4QrRftHrAl3CtYMQ426jndPNw+3j/gP+A+5Dz0ORo2ZzHtK8MlAR/DFyUQRghGAEX4Y/DA6Hrhrtp51PPOMcpIxkfDOsEqwBrACsH4wtvFqMlPzr7T39mZ4NHna+9H90b/RwcsD9QWIB7zJDErwjCPNYU5kzyuPs4/7D8KPys9VTqVNvoxlix/Js0fmxgGES0JLwEt+UbxmulH4m3bJdWKz7HKr8aTw2nBPMAPwOLAtMJ7xS7Jvc0X0yTZzt/65onuYPZc/l8GSQ75FVEdMySEKiowDjUdOUc8fj66P/Y/MT9uPbQ6DjeLMjwtOCeWIHIZ5xEUChgCFfop8nXqF+Mt3NTVI9A0yxjH4cOcwVHAB8C9wHLCHsW3yC7NcdJs2AXfI+ap7Xn1c/12BWUNHRWBHHIj1CmPL4o0sjj3O0o+oz/8P1Q/rT0PO4M3GDPgLfAnXiFHGsYS+goCA/76DvNR6+jj8NyE1sDQucuExzPE0cFqwALAnMA0wsTEQ8iizM/Rtdc+3k/lyuyT9Ir8jgSADEAUrxuvIiMp8S4DNEU4ozsTPog//z90P+o9Zzv2N6Qzgi6lKCUiGxulE+AL6wPn+/LzL+y65LTdN9de0UHM88eHxArChsABwH3A+cFuxNLHGMwu0QHXed175OzrrvOh+6UDmwtiE9wa6SFvKFIuejPUN0072D1rP/4/kT8kPr07ZjgsNCEvWCnpIu4bgxTFDNME0PzY9A3tjuV63uzX/9HMzGbI38RGwqXAA8BiwMHBGsRjx5HLkNBP1rXcqeMP68nyuPq8ArUKgxIHGiMhuSevLe4yYDf0Ops9Sj/6P6o/Wj4PPNM4sjS+LwkqrCO/HF8VqQ28Bbn9vvXt7WTmQd+j2KPSWc3byDrFhcLIwAnASsCMwcnD+MYMy/XPn9Xz29niM+rl8dD50gHPCaMRMRlaIAEnCy1fMuo2mDpaPSU/8z/AP40+Xjw9OTU1WDC4Km0kjx07Fo0OpAai/qX2ze465wvgXNlJ0+nNUsmYxcjC7sARwDbAW8F8w5DGispcz/HUM9sK4ljpAvHo+OkA6AjDEFoYkB9HJmQszjFxNjg6Fj3+Puk/0z+8Pqo8ozm1Ne8wZSssJV0eFRdwD4wHjP+M967vEujW4Bfa8dN7zs3J+cUNwxfBHsAlwC3BMcMqxgzKxs5G1HXaPOF+6B/wAPgAAAAI4Q+CF8QeiyW6Kzox9DXWOc880z7bP+I/6T7zPAc6MzaFMQ8s6SUqH+4XUhB0CHQAdPiQ8Ovoo+HU2pvUEc9Lyl3GVsNEwS3AF8ACwerCyMWPyTLOnNO52XDgpuc97xj3F/8YB/4OqBb2Hc0kDyukMHY1cDmEPKU+yj/vPxI/OD1oOq42FzK3LKQm9R/GGDMRWwleAVz5c/HF6XHik9tI1ajPy8rDxqLDc8FAwA3A28CmwmjFFsmhzfXS/9im38/mXe4x9i7+MAYbDs0VJx0NJGEqCzD0NAg5Nzx0PrY/9z84P3s9xjolN6cyXS1dJ78gnBkTEkIKRwJE+lfyoepB41Tc99VC0E7LLcfxw6bBVsAGwLbAZcIMxaDIEs1R0kfY3d755X3tS/VE/UgFNw3xFFccSyOxKXAvbzSdOOY7Pz6eP/0/Wz+6PSE7mjc0MwEuFCiGIXIa8xIoCzADLfs7833rEuQX3ajW39DUy5rHQ8TcwW/AAsCVwCjCs8QsyIbMrtGR1xfeJOWe7GX0W/xfBFIMFBSFG4ci/yjSLugzLjiSOwc+gz//P3o/9j15Ow04vzOiLskoTCJGG9ETDgwZBBX8IPRb7OXk291b137RXMwKyJnEFsKMwAHAeMDtwV3Eu8f9yw/R3dZR3VHkwOuA83L7dgNtCzYTsRrCIUsoMS5eM703PDvMPWQ//j+WPy8+zTt8OEc0QC98KRAjGByvFPIMAgX+/Ab1Ou255aLeENgg0ujMfcjxxFPCrMAEwF3AtsEJxE7Hdstx0CzWjtx/4+Pqm/KK+o0ChwpXEt0Z+yCUJ48t0jJJN+I6jj1DP/k/rz9kPh886DjMNN0vLCrTI+kcixXXDesF6P3s9RnujuZq38jYxNJ1zfLITMWSws/ACsBGwILBucPjxvLK1s981c3br+IH6rfxofmkAaAJdxEGGTIg3CbpLEMy0jaFOkw9Hj/xP8Q/lz5tPFE5TzV2MNsqkyS5HWYWug7TBtH+0/b67mXnM+CB2WrTBs5ryavF1cL2wBTAMsBSwW3De8Zxyj7Pz9QN2+DhLOnU8Ln4ugC5CJUQLxhnHyEmQiyxMVg2JToIPfY+5j/WP8Y+uTy4Oc81DTGHK1Ilhh5AF50Puwe6/7r32+896P/gPdoT1JnO5skMxhzDIMEgwCLAJMEjwxbG8smoziTUUNoT4VPo8u/R99L/0ge0D1YXmx5lJZgrHDHbNcI5wDzKPtg/5T/xPgE9GzpMNqIxMSwOJlMfGRh/EKIIowCi+L7wF+nM4fravtQvz2TKccZlw03BMcAVwPrA3MK0xXfJFc5705TZSOB75xDv6vbo/uoG0Q58Fs0dpiTsKoYwXDVcOXU8mz7GP/E/Gj9GPXs6xjY0MtksySYeIPEYYBGJCYwBivmh8fHpmuK622vVx8/lytjGssN9wUTAC8DTwJnCVsX+yITN1NLa2H7fpOYw7gP2//0CBu0NoRX+HOYjPirsL9o08zgnPGk+sT/5Pz8/iD3ZOj03xDJ+LYIn5yDHGUAScAp2AnL6hfLN6mrje9wa1mLQactDxwHEscFbwATAr8BZwvrEiMj2zDDSI9i13s7lUO0d9Rb9GQUJDcUULRwkI44pUC9VNIc41js0Ppk//T9hP8Y9MzuyN1AzIS44KK4hnBofE1YLXwNb+2nzqus85D7dzNb/0O/LsMdUxOjBdcABwI/AHMKhxBXIasyO0W3X79365HHsN/Qt/DAEJAzoE1sbYCLbKLIuzTMYOIE7/D19P/8/gD8BPoo7IzjbM8Iu7Sh0InAb/hM7DEgERPxO9IfsD+UD3n/XntF4zCHIqsQiwpLAAsBywOLBTMSlx+HL79C61irdJ+SU61LzRPtHAz8LCROHGpohJigRLkIzpjcqO8A9Xj/9P5s/Oj7eO5I4YjRgL58pNyNCHNsUIA0xBS39NPVm7ePlyd412EDSBM2UyAPFX8KzwAXAWMCrwfnDOMdby1LQCNZn3Fbjt+pu8lv6XgJZCioSshnTIG8nbi21MjE3zzqBPTw/+D+zP28+Lzz9OOc0/C9PKvkjEx23FQQOGQYW/hr2Ru655pLf7djl0pLNCslfxaDC18AMwELAeMGqw87G2Mq4z1nVptuG4tvpivFz+XUBcglKEdsYCSC3JsgsJjK6NnI6Pz0WP/A/yD+gPn08ZjlpNZUw/Sq5JOIdkhboDgEHAP8B9yfvkOdc4KfZjNMjzoPJvsXjwv7AFsAvwEjBXsNnxlfKIM+s1Ofat+EB6afwi/iMAIsIaBAEGD4f/CUgLJMxPzYROvo87T7kP9k/zz7HPMw56DUrMakreCWvHmwXyg/pB+n/6fcJ8GnoKOE=';
  const NEGATIVE_BEEP = 'data:audio/wav;base64,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';

  useEffect(() => {
    positiveAudioRef.current = new Audio(POSITIVE_BEEP);
    negativeAudioRef.current = new Audio(NEGATIVE_BEEP);
    positiveAudioRef.current.volume = 0.7;
    negativeAudioRef.current.volume = 0.7;
  }, []);

  const getAudioContext = () => {
    if (!audioContextRef.current) {
      const Context = window.AudioContext || window.webkitAudioContext;
      if (!Context) return null;
      audioContextRef.current = new Context();
    }
    if (audioContextRef.current.state === 'suspended') {
      audioContextRef.current.resume();
    }
    return audioContextRef.current;
  };

  const unlockAudio = () => {
    const ctx = getAudioContext();
    if (!ctx) return;
    const buffer = ctx.createBuffer(1, 1, 22050);
    const source = ctx.createBufferSource();
    source.buffer = buffer;
    source.connect(ctx.destination);
    try {
      source.start(0);
    } catch (error) {
      // ignore playback errors
    }
    if (positiveAudioRef.current) {
      positiveAudioRef.current.play().catch(() => {});
      positiveAudioRef.current.pause();
      positiveAudioRef.current.currentTime = 0;
    }
    if (negativeAudioRef.current) {
      negativeAudioRef.current.play().catch(() => {});
      negativeAudioRef.current.pause();
      negativeAudioRef.current.currentTime = 0;
    }
  };

  const playFeedbackSound = (type) => {
    const audio = type === 'positive' ? positiveAudioRef.current : negativeAudioRef.current;
    if (audio) {
      try {
        audio.currentTime = 0;
        const playPromise = audio.play();
        if (playPromise && typeof playPromise.catch === 'function') {
          playPromise.catch(() => {});
        }
        return;
      } catch (error) {
        // fall back to WebAudio
      }
    }
    const ctx = getAudioContext();
    if (!ctx) return;
    const now = ctx.currentTime;
    const gain = ctx.createGain();
    gain.gain.setValueAtTime(0, now);
    gain.gain.linearRampToValueAtTime(0.12, now + 0.02);
    gain.gain.exponentialRampToValueAtTime(0.0001, now + 0.35);
    gain.connect(ctx.destination);

    const osc = ctx.createOscillator();
    osc.type = type === 'positive' ? 'sine' : 'triangle';
    if (type === 'positive') {
      osc.frequency.setValueAtTime(660, now);
      osc.frequency.exponentialRampToValueAtTime(980, now + 0.18);
    } else {
      osc.frequency.setValueAtTime(220, now);
      osc.frequency.exponentialRampToValueAtTime(140, now + 0.2);
    }
    osc.connect(gain);
    osc.start(now);
    osc.stop(now + 0.36);
  };

  const coerceResponseJson = (value) => {
    if (!value) return null;
    if (typeof value === 'string') {
      try {
        return JSON.parse(value);
      } catch (error) {
        return null;
      }
    }
    if (typeof value === 'object') return value;
    return null;
  };

  const scrollToDictionary = () => {
    if (dictionaryRef.current) {
      dictionaryRef.current.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
  };

  const scrollToFlashcards = () => {
    if (flashcardsRef.current) {
      flashcardsRef.current.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }
  };

  const folderColorOptions = ['#5ddcff', '#7c5cff', '#ff6b6b', '#ffd166', '#06d6a0', '#f78c6b'];
  const folderIconOptions = ['book', 'bolt', 'star', 'target', 'flag', 'check'];

  const isSectionVisible = (key) => {
    if (flashcardsOnly) {
      return key === 'flashcards';
    }
    return selectedSections.has(key);
  };

  const showHero = !flashcardsOnly && selectedSections.size === 0;

  const toggleSection = (key) => {
    setSelectedSections((prev) => {
      if (!menuMultiSelect) {
        return new Set([key]);
      }
      const next = new Set(prev);
      if (next.has(key)) {
        next.delete(key);
      } else {
        next.add(key);
      }
      return next;
    });
  };

  const ensureSectionVisible = (key) => {
    setSelectedSections((prev) => {
      const next = new Set(prev);
      next.add(key);
      return next;
    });
  };

  const openSectionAndScroll = (key, ref) => {
    ensureSectionVisible(key);
    setTimeout(() => {
      if (ref?.current) {
        ref.current.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }
    }, 80);
  };

  const openFlashcardsSetup = (ref) => {
    setFlashcardsVisible(true);
    setFlashcardsOnly(false);
    setFlashcardSessionActive(false);
    setFlashcardExitSummary(false);
    ensureSectionVisible('flashcards');
    if (ref?.current) {
      setTimeout(() => {
        ref.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }, 120);
    }
  };

  const showAllSections = () => {
    setSelectedSections(new Set(['translations', 'youtube', 'dictionary', 'flashcards', 'analytics']));
  };

  const hideAllSections = () => {
    setSelectedSections(new Set());
  };

  const handleMenuSelection = (key, ref) => {
    setSelectedSections((prev) => {
      if (!menuMultiSelect) {
        return new Set([key]);
      }
      const next = new Set(prev);
      if (next.has(key)) {
        next.delete(key);
      } else {
        next.add(key);
      }
      return next;
    });
    if (key === 'flashcards') {
      setFlashcardsVisible(true);
      setFlashcardsOnly(false);
      setFlashcardSessionActive(false);
      setFlashcardExitSummary(false);
    }
    if (!menuMultiSelect) {
      setMenuOpen(false);
    }
    if (ref?.current) {
      setTimeout(() => {
        ref.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }, 120);
    }
  };

  const resolveFolderIconLabel = (icon) => {
    const map = {
      book: 'Book',
      bolt: 'Bolt',
      star: 'Star',
      target: 'Target',
      flag: 'Flag',
      check: 'Check',
    };
    return map[icon] || 'Folder';
  };

  const renderFolderIcon = (icon, color) => {
    const fill = color || '#5ddcff';
    const stroke = '#0b1020';
    if (icon === 'bolt') {
      return (
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M13 2L4 14h6l-1 8 9-12h-6l1-8z" fill={fill} stroke={stroke} strokeWidth="1.4" />
        </svg>
      );
    }
    if (icon === 'star') {
      return (
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M12 2l2.7 5.6 6.2.9-4.5 4.4 1 6.2L12 16.8 6.6 19.1l1-6.2-4.5-4.4 6.2-.9L12 2z" fill={fill} stroke={stroke} strokeWidth="1.2" />
        </svg>
      );
    }
    if (icon === 'target') {
      return (
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <circle cx="12" cy="12" r="9" fill="none" stroke={fill} strokeWidth="2.4" />
          <circle cx="12" cy="12" r="5" fill="none" stroke={fill} strokeWidth="2.4" />
          <circle cx="12" cy="12" r="2" fill={fill} />
        </svg>
      );
    }
    if (icon === 'flag') {
      return (
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M6 3v18M6 4h11l-2 3 2 3H6" fill={fill} stroke={stroke} strokeWidth="1.4" />
        </svg>
      );
    }
    if (icon === 'check') {
      return (
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M5 12l4 4 10-10" fill="none" stroke={fill} strokeWidth="2.6" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      );
    }
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <path d="M4 6h8l2 2h6v10a2 2 0 0 1-2 2H4z" fill={fill} stroke={stroke} strokeWidth="1.2" />
      </svg>
    );
  };

  // Состояние для хранения токена доступа. Изначально его нет.
  // Мы говорим React'у: "Создай ячейку памяти. Изначально положи туда null (пустоту)".
  // Когда мы захотим обновить эту ячейку, мы будем использовать функцию setToken.
  // Каждый раз, когда мы вызываем setToken с новым значением, React "замечает" это изменение
  // и перерисовывает компонент App с новым значением token.
  // Аналогично: const [username, setUsername] = useState(''); — создали память для имени пользователя, изначально пустая строка.
  // Итог: useState — это способ "создать память" внутри функционального компонента React.
  const [token, setToken] = useState(null);

  // LiveKit login state
  const [telegramID, setTelegramID] = useState('');
  const [username, setUsername] = useState('');

  const handleConnect = async (e) => {
    e.preventDefault();
    if (!telegramID || !username) {
      alert('Пожалуйста, введите ваше имя');
      return;
    }

    try {
      const response = await fetch(
        `/api/token?user_id=${encodeURIComponent(telegramID)}&username=${encodeURIComponent(username)}`
      );

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Ошибка получения токена: ${errorText}`);
      }

      const data = await response.json();
      setToken(data.token);
    } catch (error) {
      console.error(error);
      alert(error.message);
    }
  };

  useEffect(() => {
    if (!isWebAppMode || !initData) {
      return;
    }

    const bootstrap = async () => {
      try {
        setWebappError('');
        const response = await fetch('/api/webapp/bootstrap', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ initData }),
        });
        if (!response.ok) {
          throw new Error(await response.text());
        }
        const data = await response.json();
        setSessionId(data.session_id);
        setWebappUser(data.user);
      } catch (error) {
        setWebappError(`Ошибка инициализации: ${error.message}`);
      }
    };

    bootstrap();
  }, [initData, isWebAppMode]);

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    if (params.get('review') === '1') {
      setFlashcardsVisible(true);
      setFlashcardsOnly(true);
      setFlashcardSessionActive(true);
    }
    const startParam = telegramApp?.initDataUnsafe?.start_param;
    if (startParam === 'review' || startParam === 'flashcards') {
      setFlashcardsVisible(true);
      setFlashcardsOnly(true);
      setFlashcardSessionActive(true);
    }
    if (window.location.pathname === '/webapp/review') {
      setFlashcardsVisible(true);
      setFlashcardsOnly(true);
      setFlashcardSessionActive(true);
    }
  }, []);

  useEffect(() => {
    const storedAvatar = safeStorageGet('webapp_avatar');
    if (storedAvatar) {
      setUserAvatar(storedAvatar);
    }
  }, []);

  useEffect(() => {
    if (!isWebAppMode || !initData) {
      return;
    }
    loadFolders();
  }, [initData, isWebAppMode]);

  useEffect(() => {
    if (!flashcardSessionActive) {
      return;
    }
    loadFlashcards();
    scrollToFlashcards();
  }, [flashcardSessionActive, initData, flashcardFolderMode, flashcardFolderId, flashcardSetSize]);

  useEffect(() => {
    if (!flashcards.length) {
      setFlashcardOptions([]);
      return;
    }
    const entry = flashcards[flashcardIndex];
    setFlashcardOptions(buildFlashcardOptions(entry, flashcards));
    setFlashcardSelection(null);
  }, [flashcards, flashcardIndex]);

  useEffect(() => {
    flashcardIndexRef.current = flashcardIndex;
  }, [flashcardIndex]);

  useEffect(() => {
    flashcardSelectionRef.current = flashcardSelection;
  }, [flashcardSelection]);

  useEffect(() => {
    if (!flashcardSessionActive || flashcards.length === 0) {
      return;
    }
    setFlashcardTimerDurationSec(flashcardDurationSec);
    setFlashcardTimerKey((prev) => prev + 1);
  }, [flashcardIndex, flashcardSessionActive, flashcards.length, flashcardDurationSec]);

  useEffect(() => {
    if (!flashcardSessionActive || flashcardSetComplete || flashcardExitSummary || !flashcards.length) {
      return;
    }
    if (autoAdvanceTimeoutRef.current) {
      clearTimeout(autoAdvanceTimeoutRef.current);
    }
    setFlashcardTimedOut(false);
    autoAdvanceTimeoutRef.current = setTimeout(() => {
      const currentIndex = flashcardIndexRef.current;
      const currentSelection = flashcardSelectionRef.current;
      if (currentSelection !== null) return;
      const entry = flashcards[currentIndex];
      if (!entry) return;
      recordFlashcardAnswer(entry.id, false);
      setFlashcardStats((prev) => ({
        ...prev,
        wrong: prev.wrong + 1,
      }));
      setFlashcardTimedOut(true);
      setFlashcardSelection(-1);
      if (revealTimeoutRef.current) {
        clearTimeout(revealTimeoutRef.current);
      }
      revealTimeoutRef.current = setTimeout(() => {
        advanceFlashcard();
      }, 3000);
    }, flashcardTimerDurationSec * 1000);

    return () => {
      if (autoAdvanceTimeoutRef.current) {
        clearTimeout(autoAdvanceTimeoutRef.current);
        autoAdvanceTimeoutRef.current = null;
      }
    };
  }, [
    flashcardSessionActive,
    flashcardSetComplete,
    flashcardExitSummary,
    flashcards,
    flashcardIndex,
    flashcardTimerDurationSec,
  ]);

  useEffect(() => {
    if (!flashcardSessionActive && autoAdvanceTimeoutRef.current) {
      clearTimeout(autoAdvanceTimeoutRef.current);
      autoAdvanceTimeoutRef.current = null;
    }
    if (!flashcardSessionActive && revealTimeoutRef.current) {
      clearTimeout(revealTimeoutRef.current);
      revealTimeoutRef.current = null;
    }
  }, [flashcardSessionActive]);

  const loadSentences = async () => {
    if (!initData) {
      return;
    }
    try {
      setFinishMessage('');
      const response = await fetch('/api/webapp/sentences', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, limit: 7 }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const data = await response.json();
      setSentences(data.items || []);
      setResults([]);
      setFinishStatus('idle');
    } catch (error) {
      setWebappError(`Ошибка загрузки предложений: ${error.message}`);
    }
  };

  const loadTopics = async () => {
    setTopicsLoading(true);
    setTopicsError('');
    try {
      const response = await fetch('/api/webapp/topics');
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const data = await response.json();
      const items = Array.isArray(data.items) ? data.items : [];
      setTopics(items);
      if (items.length > 0 && !items.includes(selectedTopic)) {
        setSelectedTopic(items[0]);
      }
    } catch (error) {
      setTopicsError(`Ошибка тем: ${error.message}`);
    } finally {
      setTopicsLoading(false);
    }
  };

  useEffect(() => {
    if (!webappUser?.id || sentences.length === 0) {
      return;
    }
    const storageKey = `webappDrafts_${webappUser.id}_${sessionId || 'nosession'}`;
    const stored = safeStorageGet(storageKey);
    const sentenceIds = sentences.map((item) => String(item.id_for_mistake_table));
    let initial = sentenceIds.reduce((acc, id) => ({ ...acc, [id]: '' }), {});
    if (stored) {
      try {
        const parsed = JSON.parse(stored);
        if (parsed && typeof parsed === 'object') {
          initial = sentenceIds.reduce((acc, id) => ({
            ...acc,
            [id]: parsed[id] || '',
          }), {});
        }
      } catch (error) {
        console.warn('Failed to parse saved drafts', error);
      }
    }
    setTranslationDrafts(initial);
  }, [sentences, webappUser?.id, sessionId]);

  useEffect(() => {
    if (!webappUser?.id || Object.keys(translationDrafts).length === 0) {
      return;
    }
    const storageKey = `webappDrafts_${webappUser.id}_${sessionId || 'nosession'}`;
    safeStorageSet(storageKey, JSON.stringify(translationDrafts));
  }, [translationDrafts, webappUser?.id, sessionId]);

  useEffect(() => {
    if (isWebAppMode && initData) {
      loadTopics();
      loadSentences();
    }
  }, [initData, isWebAppMode]);

  useEffect(() => {
    const stored = safeStorageGet('webapp_youtube');
    if (!stored) return;
    try {
      const parsed = JSON.parse(stored);
      if (parsed?.input) {
        setYoutubeInput(parsed.input);
      }
      if (parsed?.id) {
        setYoutubeId(parsed.id);
      }
    } catch (error) {
      // ignore
    }
  }, []);

  const handleWebappSubmit = async (event) => {
    event.preventDefault();
    if (!initData) {
      setWebappError('initData не найдено. Откройте Web App внутри Telegram.');
      return;
    }
    if (sentences.length === 0) {
      setWebappError('Нет предложений для перевода.');
      return;
    }
    if (Object.values(translationDrafts).every((text) => !text.trim())) {
      setWebappError('Заполните хотя бы один перевод.');
      return;
    }

    const numberedOriginal = sentences
      .map((item) => `${item.unique_id ?? item.id_for_mistake_table}. ${item.sentence}`)
      .join('\n');
    const numberedTranslations = sentences
      .map((item) => {
        const translation = translationDrafts[String(item.id_for_mistake_table)] || '';
        return `${item.unique_id ?? item.id_for_mistake_table}. ${translation}`;
      })
      .join('\n');

    setWebappLoading(true);
    setWebappError('');
    setResults([]);
    setExplanations({});
    setExplanationLoading({});

    try {
      const submittedIds = Object.entries(translationDrafts)
        .filter(([, text]) => text && text.trim())
        .map(([id]) => Number(id));
      const response = await fetch('/api/message', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          session_id: sessionId,
          translations: Object.entries(translationDrafts).map(([id, translation]) => ({
            id_for_mistake_table: Number(id),
            translation,
          })),
          original_text: numberedOriginal,
          user_translation: numberedTranslations,
        }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const data = await response.json();
      setResults(data.results || []);
      if (submittedIds.length > 0) {
        setSentences((prev) => prev.filter((item) => !submittedIds.includes(Number(item.id_for_mistake_table))));
        setTranslationDrafts((prev) => {
          const next = { ...prev };
          submittedIds.forEach((id) => {
            delete next[String(id)];
          });
          const storageKey = `webappDrafts_${webappUser?.id || 'unknown'}_${sessionId || 'nosession'}`;
          if (Object.keys(next).length === 0) {
            safeStorageRemove(storageKey);
          } else {
            safeStorageSet(storageKey, JSON.stringify(next));
          }
          return next;
        });
      }
    } catch (error) {
      setWebappError(`Ошибка проверки: ${error.message}`);
    } finally {
      setWebappLoading(false);
    }
  };

  const handleStartTranslation = async () => {
    if (!initData) {
      setWebappError('initData не найдено. Откройте Web App внутри Telegram.');
      return;
    }
    setWebappLoading(true);
    setWebappError('');
    setFinishMessage('');
    setFinishStatus('idle');
    try {
      const response = await fetch('/api/webapp/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          topic: selectedTopic,
        }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      await response.json();
      await loadSentences();
    } catch (error) {
      setWebappError(`Ошибка старта: ${error.message}`);
    } finally {
      setWebappLoading(false);
    }
  };

  const handleDraftChange = (sentenceId, value) => {
    setTranslationDrafts((prev) => ({
      ...prev,
      [String(sentenceId)]: value,
    }));
  };

  const normalizeSelectionText = (value) => {
    if (!value) return '';
    return value.replace(/\s+/g, ' ').trim();
  };

  const resolveDictionaryDirection = (item) => {
    if (!item) return 'ru-de';
    if (item.translation_de) return 'ru-de';
    if (item.translation_ru) return 'de-ru';
    if (item.word_de) return 'de-ru';
    return 'ru-de';
  };

  const hasCyrillic = (value) => /[А-Яа-яЁё]/.test(value || '');

  const handleSelection = (event, overrideText = '') => {
    const text = overrideText || normalizeSelectionText(window.getSelection()?.toString() || '');
    if (!text) {
      setSelectionText('');
      setSelectionPos(null);
      return;
    }
    const clientX = event?.clientX ?? event?.touches?.[0]?.clientX ?? window.innerWidth / 2;
    const clientY = event?.clientY ?? event?.touches?.[0]?.clientY ?? window.innerHeight / 3;
    setSelectionText(text);
    setSelectionPos({ x: clientX, y: clientY });
  };

  const clearSelection = () => {
    setSelectionText('');
    setSelectionPos(null);
  };

  const handleQuickAddToDictionary = async (text) => {
    const cleaned = normalizeSelectionText(text);
    if (!cleaned) return;
    if (!initData) {
      setDictionaryError('initData не найдено. Откройте Web App внутри Telegram.');
      return;
    }
    let normalized = cleaned;
    if (!hasCyrillic(cleaned)) {
      try {
        const normalizeResponse = await fetch('/api/webapp/normalize/de', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ initData, text: cleaned }),
        });
        if (normalizeResponse.ok) {
          const data = await normalizeResponse.json();
          normalized = data.normalized || cleaned;
        }
      } catch (error) {
        // ignore normalization errors
      }
    }
    setDictionaryLoading(true);
    setDictionaryError('');
    setDictionarySaved('');
    setDictionaryWord(normalized);
    setLastLookupScrollY(window.scrollY);
    try {
      const response = await fetch('/api/webapp/dictionary', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, word: normalized }),
      });
      if (!response.ok) {
        let message = await response.text();
        try {
          const data = JSON.parse(message);
          message = data.error || message;
        } catch (error) {
          // ignore parsing errors
        }
        throw new Error(message);
      }
      const data = await response.json();
      setDictionaryResult(data.item || null);
      const detectedDirection = data.direction || resolveDictionaryDirection(data.item);
      setDictionaryDirection(detectedDirection);
      scrollToDictionary();

      const saveResponse = await fetch('/api/webapp/dictionary/save', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          word_ru: detectedDirection === 'ru-de' ? normalized : '',
          word_de: detectedDirection === 'de-ru' ? normalized : '',
          translation_de: data.item?.translation_de || '',
          translation_ru: data.item?.translation_ru || '',
          response_json: data.item || {},
          folder_id: dictionaryFolderId !== 'none' ? dictionaryFolderId : null,
        }),
      });
      if (!saveResponse.ok) {
        let message = await saveResponse.text();
        try {
          const payload = JSON.parse(message);
          message = payload.error || message;
        } catch (error) {
          // ignore parsing errors
        }
        throw new Error(message);
      }
      setDictionarySaved('Добавлено в словарь ✅');
      clearSelection();
    } catch (error) {
      setDictionaryError(`Ошибка сохранения: ${error.message}`);
    } finally {
      setDictionaryLoading(false);
    }
  };

  const handleQuickLookupDictionary = async (text) => {
    const cleaned = normalizeSelectionText(text);
    if (!cleaned) return;
    if (!initData) {
      setDictionaryError('initData не найдено. Откройте Web App внутри Telegram.');
      return;
    }
    let normalized = cleaned;
    if (!hasCyrillic(cleaned)) {
      try {
        const normalizeResponse = await fetch('/api/webapp/normalize/de', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ initData, text: cleaned }),
        });
        if (normalizeResponse.ok) {
          const data = await normalizeResponse.json();
          normalized = data.normalized || cleaned;
        }
      } catch (error) {
        // ignore normalization errors
      }
    }
    setDictionaryLoading(true);
    setDictionaryError('');
    setDictionarySaved('');
    setDictionaryWord(normalized);
    setLastLookupScrollY(window.scrollY);
    try {
      const response = await fetch('/api/webapp/dictionary', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, word: normalized }),
      });
      if (!response.ok) {
        let message = await response.text();
        try {
          const data = JSON.parse(message);
          message = data.error || message;
        } catch (error) {
          // ignore parsing errors
        }
        throw new Error(message);
      }
      const data = await response.json();
      setDictionaryResult(data.item || null);
      setDictionaryDirection(data.direction || resolveDictionaryDirection(data.item));
      scrollToDictionary();
      clearSelection();
    } catch (error) {
      setDictionaryError(`Ошибка словаря: ${error.message}`);
    } finally {
      setDictionaryLoading(false);
    }
  };

  const loadFlashcards = async () => {
    if (!initData) {
      setFlashcardsError('initData не найдено. Откройте Web App внутри Telegram.');
      return;
    }
    setFlashcardsLoading(true);
    setFlashcardsError('');
    try {
      const response = await fetch('/api/webapp/flashcards/set', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          set_size: flashcardSetSize,
          wrong_size: 5,
          folder_mode: flashcardFolderMode,
          folder_id: flashcardFolderMode === 'folder' && flashcardFolderId ? flashcardFolderId : null,
        }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const data = await response.json();
      const items = (data.items || []).map((item) => ({
        ...item,
        response_json: coerceResponseJson(item.response_json),
      }));
      if (autoAdvanceTimeoutRef.current) {
        clearTimeout(autoAdvanceTimeoutRef.current);
        autoAdvanceTimeoutRef.current = null;
      }
      if (revealTimeoutRef.current) {
        clearTimeout(revealTimeoutRef.current);
        revealTimeoutRef.current = null;
      }
      setFlashcards(items);
      setFlashcardIndex(0);
      setFlashcardSelection(null);
      setFlashcardSetComplete(false);
      setFlashcardTimedOut(false);
      setFlashcardExitSummary(false);
      setFlashcardTimerDurationSec(flashcardDurationSec);
      setFlashcardTimerKey((prev) => prev + 1);
      setFlashcardStats({
        total: items.length,
        correct: 0,
        wrong: 0,
      });

      const poolResponse = await fetch('/api/webapp/dictionary/cards', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          limit: 60,
          folder_mode: flashcardFolderMode,
          folder_id: flashcardFolderMode === 'folder' && flashcardFolderId ? flashcardFolderId : null,
        }),
      });
      if (poolResponse.ok) {
        const poolData = await poolResponse.json();
        const poolItems = (poolData.items || []).map((item) => ({
          ...item,
          response_json: coerceResponseJson(item.response_json),
        }));
        setFlashcardPool(poolItems);
      }
    } catch (error) {
      setFlashcardsError(`Ошибка карточек: ${error.message}`);
    } finally {
      setFlashcardsLoading(false);
    }
  };

  const loadFolders = async () => {
    if (!initData) {
      return;
    }
    setFoldersLoading(true);
    setFoldersError('');
    try {
      const response = await fetch('/api/webapp/dictionary/folders', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const data = await response.json();
      setFolders(data.items || []);
    } catch (error) {
      setFoldersError(`Ошибка папок: ${error.message}`);
    } finally {
      setFoldersLoading(false);
    }
  };

  const handleCreateFolder = async () => {
    if (!initData) {
      setFoldersError('initData не найдено. Откройте Web App внутри Telegram.');
      return;
    }
    if (!newFolderName.trim()) {
      setFoldersError('Введите название папки.');
      return;
    }
    setFoldersLoading(true);
    setFoldersError('');
    try {
      const response = await fetch('/api/webapp/dictionary/folders/create', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          name: newFolderName.trim(),
          color: newFolderColor,
          icon: newFolderIcon,
        }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const data = await response.json();
      const created = data.item;
      setFolders((prev) => [created, ...prev]);
      setDictionaryFolderId(String(created.id));
      setShowNewFolderForm(false);
      setNewFolderName('');
    } catch (error) {
      setFoldersError(`Ошибка создания папки: ${error.message}`);
    } finally {
      setFoldersLoading(false);
    }
  };

  const shuffleArray = (items) => {
    const array = [...items];
    const rand = () => {
      if (window.crypto?.getRandomValues) {
        const buf = new Uint32Array(1);
        window.crypto.getRandomValues(buf);
        return buf[0] / 2 ** 32;
      }
      return Math.random();
    };
    for (let i = array.length - 1; i > 0; i -= 1) {
      const j = Math.floor(rand() * (i + 1));
      [array[i], array[j]] = [array[j], array[i]];
    }
    return array;
  };

  const buildFlashcardOptions = (entry, allEntries) => {
    const responseJson = entry?.response_json || {};
    const correct = entry?.translation_de
      || responseJson.translation_de
      || entry?.translation_ru
      || responseJson.translation_ru
      || '';
    if (!correct) return [];
    const pool = [...allEntries, ...flashcardPool]
      .filter((item) => item && item.id !== entry?.id);
    const values = Array.from(new Set(
      pool
        .map((item) => (
          item.translation_de
            || item.response_json?.translation_de
            || item.translation_ru
            || item.response_json?.translation_ru
            || ''
        ))
        .filter(Boolean)
        .filter((value) => value !== correct)
    ));

    const distractors = [];
    while (distractors.length < 3 && values.length > 0) {
      const index = Math.floor(Math.random() * values.length);
      distractors.push(values.splice(index, 1)[0]);
    }

    const options = Array.from(new Set([correct, ...distractors]));
    while (options.length < 4 && values.length > 0) {
      options.push(values.shift());
    }
    while (options.length < 4) {
      options.push(correct);
    }
    return shuffleArray(options).slice(0, 4);
  };

  const recordFlashcardAnswer = async (entryId, isCorrect) => {
    if (!initData || !entryId) {
      return;
    }
    try {
      await fetch('/api/webapp/flashcards/answer', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, entry_id: entryId, is_correct: isCorrect }),
      });
    } catch (error) {
      // ignore answer tracking errors
    }
  };

  const advanceFlashcard = () => {
    if (autoAdvanceTimeoutRef.current) {
      clearTimeout(autoAdvanceTimeoutRef.current);
      autoAdvanceTimeoutRef.current = null;
    }
    const nextIndex = flashcardIndex + 1;
    if (nextIndex >= flashcards.length) {
      setFlashcardSetComplete(true);
      return;
    }
    setFlashcardIndex(nextIndex);
    setFlashcardSelection(null);
  };

  const handleAvatarUpload = (event) => {
    const file = event.target.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => {
      const result = typeof reader.result === 'string' ? reader.result : '';
      if (!result) return;
      setUserAvatar(result);
      safeStorageSet('webapp_avatar', result);
    };
    reader.readAsDataURL(file);
  };

  const renderClickableText = (text) => {
    if (!text) return null;
    return text.split(/\s+/).map((word, index) => {
      const cleaned = word.replace(/[^A-Za-zÄÖÜäöüßÀ-ÿ'’-]/g, '');
      if (!cleaned) {
        return <span key={`w-${index}`}>{word} </span>;
      }
      return (
        <span
          key={`w-${index}`}
          className="clickable-word"
          onClick={(event) => handleSelection(event, cleaned)}
        >
          {word}{' '}
        </span>
      );
    });
  };

  const parseTranscriptInput = (value) => {
    const lines = value
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean)
      .filter((line) => !/^\d+$/.test(line))
      .filter((line) => !/^\d{2}:\d{2}:\d{2}/.test(line))
      .filter((line) => !/^\d{2}:\d{2}/.test(line))
      .filter((line) => !/-->/.test(line))
      .filter((line) => !/^WEBVTT/i.test(line));
    return lines.map((line, index) => ({
      text: line,
      start: index,
      duration: 0,
    }));
  };

  const handleManualTranscript = () => {
    const parsed = parseTranscriptInput(manualTranscript);
    setYoutubeTranscript(parsed);
    setYoutubeTranscriptError('');
  };

  const handleFinishTranslation = async () => {
    if (!initData) {
      setWebappError('initData не найдено. Откройте Web App внутри Telegram.');
      return;
    }
    setWebappLoading(true);
    setWebappError('');
    setFinishMessage('');
    setFinishStatus('idle');
    try {
      const response = await fetch('/api/webapp/finish', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData }),
      });
      if (!response.ok) {
        let message = await response.text();
        try {
          const data = JSON.parse(message);
          message = data.error || message;
        } catch (error) {
          // ignore parsing errors
        }
        throw new Error(message);
      }
      const data = await response.json();
      setFinishMessage(data.message || 'Перевод завершён.');
      setFinishStatus('done');
      const storageKey = `webappDrafts_${webappUser?.id || 'unknown'}_${sessionId || 'nosession'}`;
      safeStorageRemove(storageKey);
      setTranslationDrafts({});
      await loadSentences();
    } catch (error) {
      setWebappError(`Ошибка завершения: ${error.message}`);
    } finally {
      setWebappLoading(false);
    }
  };

  const handleExplainTranslation = async (item) => {
    if (!initData) {
      setWebappError('initData не найдено. Откройте Web App внутри Telegram.');
      return;
    }
    const key = String(item.sentence_number ?? item.original_text);
    setExplanationLoading((prev) => ({ ...prev, [key]: true }));
    try {
      const response = await fetch('/api/webapp/explain', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          original_text: item.original_text,
          user_translation: item.user_translation,
        }),
      });
      if (!response.ok) {
        let message = await response.text();
        try {
          const data = JSON.parse(message);
          message = data.error || message;
        } catch (error) {
          // ignore parsing errors
        }
        throw new Error(message);
      }
      const data = await response.json();
      setExplanations((prev) => ({ ...prev, [key]: data.explanation }));
    } catch (error) {
      setWebappError(`Ошибка объяснения: ${error.message}`);
    } finally {
      setExplanationLoading((prev) => ({ ...prev, [key]: false }));
    }
  };

  const renderRichText = (text) => {
    if (!text) return '';
    const escaped = text
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
    return escaped
      .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
      .replace(/\*(.+?)\*/g, '<strong>$1</strong>');
  };

  const renderFeedback = (feedback) => {
    if (!feedback) return null;
    const lines = feedback.split('\n').map((line) => line.trim()).filter(Boolean);
    return lines.map((line, index) => {
      const match = line.match(/Correct Translation:\*?\s*(.+)$/i);
      if (match) {
        return (
          <div
            key={`fb-${index}`}
            className="webapp-feedback-line"
            onMouseUp={handleSelection}
          >
            <span className="webapp-feedback-label">🟣 Correct Translation:</span>
            <span className="webapp-feedback-value">{renderClickableText(match[1])}</span>
          </div>
        );
      }
      return (
        <div
          key={`fb-${index}`}
          className="webapp-feedback-line"
          dangerouslySetInnerHTML={{ __html: renderRichText(line) }}
        />
      );
    });
  };


  const handleDictionaryLookup = async (event) => {
    event.preventDefault();
    if (!initData) {
      setDictionaryError('initData не найдено. Откройте Web App внутри Telegram.');
      return;
    }
    if (!dictionaryWord.trim()) {
      setDictionaryError('Введите слово или фразу для словаря.');
      return;
    }
    setDictionaryLoading(true);
    setDictionaryError('');
    setDictionaryResult(null);
    setDictionarySaved('');
    setLastLookupScrollY(null);
    try {
      const response = await fetch('/api/webapp/dictionary', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, word: dictionaryWord.trim() }),
      });
      if (!response.ok) {
        let message = await response.text();
        try {
          const data = JSON.parse(message);
          message = data.error || message;
        } catch (error) {
          // ignore parsing errors
        }
        throw new Error(message);
      }
      const data = await response.json();
      setDictionaryResult(data.item || null);
      setDictionaryDirection(data.direction || resolveDictionaryDirection(data.item));
    } catch (error) {
      setDictionaryError(`Ошибка словаря: ${error.message}`);
    } finally {
      setDictionaryLoading(false);
    }
  };

  const handleDictionarySave = async () => {
    if (!initData) {
      setDictionaryError('initData не найдено. Откройте Web App внутри Telegram.');
      return;
    }
    if (!dictionaryResult) {
      setDictionaryError('Сначала выполните перевод в словаре.');
      return;
    }
    setCollocationsVisible(true);
    setCollocationsLoading(true);
    setCollocationsError('');
    setCollocationOptions([]);
    setSelectedCollocation(null);
    try {
      const response = await fetch('/api/webapp/dictionary/collocations', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          word: dictionaryWord.trim() || dictionaryResult.word_ru || dictionaryResult.word_de,
          translation: dictionaryDirection === 'ru-de'
            ? (dictionaryResult.translation_de || '')
            : (dictionaryResult.translation_ru || ''),
          direction: dictionaryDirection,
        }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const data = await response.json();
      const baseSource = dictionaryDirection === 'ru-de'
        ? (dictionaryResult.word_ru || dictionaryWord.trim())
        : (dictionaryResult.word_de || dictionaryWord.trim());
      const baseTarget = dictionaryDirection === 'ru-de'
        ? (dictionaryResult.translation_de || '')
        : (dictionaryResult.translation_ru || '');
      const options = [
        { source: baseSource, target: baseTarget, isBase: true },
        ...(data.items || []).map((item) => ({
          source: item.source,
          target: item.target,
          isBase: false,
        })),
      ].filter((item) => item.source && item.target);
      setCollocationOptions(options);
      setSelectedCollocation(options[0] || null);
    } catch (error) {
      setCollocationsError(`Ошибка связок: ${error.message}`);
    } finally {
      setCollocationsLoading(false);
    }
  };

  const handleConfirmSaveCollocation = async () => {
    if (!selectedCollocation) {
      setCollocationsError('Выберите вариант для сохранения.');
      return;
    }
    setDictionaryLoading(true);
    setDictionaryError('');
    setDictionarySaved('');
    try {
      const response = await fetch('/api/webapp/dictionary/save', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          word_ru: dictionaryDirection === 'ru-de' ? selectedCollocation.source : '',
          word_de: dictionaryDirection === 'de-ru' ? selectedCollocation.source : '',
          translation_de: dictionaryDirection === 'ru-de' ? selectedCollocation.target : '',
          translation_ru: dictionaryDirection === 'de-ru' ? selectedCollocation.target : '',
          response_json: dictionaryResult,
          folder_id: dictionaryFolderId !== 'none' ? dictionaryFolderId : null,
        }),
      });
      if (!response.ok) {
        let message = await response.text();
        try {
          const data = JSON.parse(message);
          message = data.error || message;
        } catch (error) {
          // ignore parsing errors
        }
        throw new Error(message);
      }
      setDictionarySaved('Добавлено в словарь ✅');
      setCollocationsVisible(false);
    } catch (error) {
      setDictionaryError(`Ошибка сохранения: ${error.message}`);
    } finally {
      setDictionaryLoading(false);
    }
  };

  const handleExportDictionaryPdf = async () => {
    if (!initData) {
      setDictionaryError('initData не найдено. Откройте Web App внутри Telegram.');
      return;
    }
    setExportLoading(true);
    setDictionaryError('');
    try {
      const response = await fetch('/api/webapp/dictionary/export/pdf', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          initData,
          folder_mode: dictionaryFolderId !== 'none' ? 'folder' : 'all',
          folder_id: dictionaryFolderId !== 'none' ? dictionaryFolderId : null,
        }),
      });
      if (!response.ok) {
        throw new Error(await response.text());
      }
      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = 'dictionary.pdf';
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(url);
    } catch (error) {
      setDictionaryError(`Ошибка выгрузки PDF: ${error.message}`);
    } finally {
      setExportLoading(false);
    }
  };

  const selectedDictionaryFolder = folders.find(
    (folder) => String(folder.id) === dictionaryFolderId
  );

  const extractYoutubeId = (value) => {
    if (!value) return '';
    const trimmed = value.trim();
    if (/^[a-zA-Z0-9_-]{11}$/.test(trimmed)) {
      return trimmed;
    }
    const patterns = [
      /v=([a-zA-Z0-9_-]{11})/,
      /youtu\.be\/([a-zA-Z0-9_-]{11})/,
      /embed\/([a-zA-Z0-9_-]{11})/,
      /shorts\/([a-zA-Z0-9_-]{11})/,
    ];
    for (const pattern of patterns) {
      const match = trimmed.match(pattern);
      if (match) return match[1];
    }
    return '';
  };

  useEffect(() => {
    const trimmed = youtubeInput.trim();
    if (!trimmed) {
      setYoutubeId('');
      setYoutubeError('');
      safeStorageRemove('webapp_youtube');
      return;
    }
    const id = extractYoutubeId(trimmed);
    if (id) {
      setYoutubeId(id);
      setYoutubeError('');
      safeStorageSet('webapp_youtube', JSON.stringify({ input: trimmed, id }));
    } else if (trimmed.length > 8) {
      setYoutubeError('Не удалось распознать ссылку или ID видео.');
      setYoutubeId('');
    } else {
      setYoutubeError('');
    }
  }, [youtubeInput]);

  useEffect(() => {
    const fetchTranscript = async () => {
      if (!youtubeId || !initData) {
        setYoutubeTranscript([]);
        setYoutubeTranscriptError('');
        return;
      }
      setYoutubeTranscriptLoading(true);
      setYoutubeTranscriptError('');
      try {
        const response = await fetch('/api/webapp/youtube/transcript', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ initData, videoId: youtubeId }),
        });
        if (!response.ok) {
          let message = await response.text();
          try {
            const data = JSON.parse(message);
            message = data.error || message;
          } catch (error) {
            // ignore parsing errors
          }
          throw new Error(message);
        }
        const data = await response.json();
        setYoutubeTranscript(data.items || []);
        setManualTranscript('');
      } catch (error) {
        setYoutubeTranscript([]);
        setYoutubeTranscriptError(`Авто-субтитры недоступны: ${error.message}`);
      } finally {
        setYoutubeTranscriptLoading(false);
      }
    };

    fetchTranscript();
  }, [youtubeId, initData]);

  const handleLoadDailyHistory = async () => {
    if (!initData) {
      setHistoryError('initData не найдено. Откройте Web App внутри Telegram.');
      return;
    }
    setHistoryLoading(true);
    setHistoryError('');
    try {
      const response = await fetch('/api/webapp/history/daily', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, limit: 50 }),
      });
      if (!response.ok) {
        let message = await response.text();
        try {
          const data = JSON.parse(message);
          message = data.error || message;
        } catch (error) {
          // ignore parsing errors
        }
        throw new Error(message);
      }
      const data = await response.json();
      setHistoryItems(data.items || []);
      setHistoryVisible(true);
    } catch (error) {
      setHistoryError(`Ошибка загрузки истории: ${error.message}`);
    } finally {
      setHistoryLoading(false);
    }
  };

  const resolveAnalyticsGranularity = (periodValue) => {
    switch (periodValue) {
      case 'day':
      case 'week':
        return 'day';
      case 'month':
        return 'week';
      case 'quarter':
      case 'half-year':
      case 'year':
      case 'all':
        return 'month';
      default:
        return 'day';
    }
  };

  const loadAnalytics = async (overridePeriod) => {
    if (!initData) {
      setAnalyticsError('initData не найдено. Откройте Web App внутри Telegram.');
      return;
    }
    const period = overridePeriod || analyticsPeriod;
    const granularity = resolveAnalyticsGranularity(period);
    setAnalyticsLoading(true);
    setAnalyticsError('');
    try {
      const summaryResponse = await fetch('/api/webapp/analytics/summary', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, period }),
      });
      if (!summaryResponse.ok) {
        throw new Error(await summaryResponse.text());
      }
      const summaryData = await summaryResponse.json();
      setAnalyticsSummary(summaryData.summary || null);

      const seriesResponse = await fetch('/api/webapp/analytics/timeseries', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, period, granularity }),
      });
      if (!seriesResponse.ok) {
        throw new Error(await seriesResponse.text());
      }
      const seriesData = await seriesResponse.json();
      setAnalyticsPoints(seriesData.points || []);

      const compareResponse = await fetch('/api/webapp/analytics/compare', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ initData, period, limit: 8 }),
      });
      if (!compareResponse.ok) {
        throw new Error(await compareResponse.text());
      }
      const compareData = await compareResponse.json();
      setAnalyticsCompare(compareData.items || []);
      setAnalyticsRank(compareData.self?.rank ?? null);
    } catch (error) {
      setAnalyticsError(`Ошибка аналитики: ${error.message}`);
    } finally {
      setAnalyticsLoading(false);
    }
  };

  useEffect(() => {
    if (!isWebAppMode || !initData) {
      return;
    }
    if (!flashcardsOnly && isSectionVisible('analytics')) {
      loadAnalytics();
    }
  }, [initData, isWebAppMode, analyticsPeriod, selectedSections, flashcardsOnly]);

  useEffect(() => {
    if (!analyticsTrendRef.current) {
      return;
    }
    const chart = echarts.init(analyticsTrendRef.current);
    const labels = analyticsPoints.map((item) => item.period_start);
    const success = analyticsPoints.map((item) => item.successful_translations || 0);
    const fail = analyticsPoints.map((item) => item.unsuccessful_translations || 0);
    const avgScore = analyticsPoints.map((item) => item.avg_score || 0);
    const avgTime = analyticsPoints.map((item) => item.avg_time_min || 0);

    chart.setOption({
      backgroundColor: 'transparent',
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'shadow' },
        formatter: (params) => {
          const map = {};
          params.forEach((entry) => {
            map[entry.seriesName] = entry.value;
          });
          const index = params[0]?.dataIndex ?? 0;
          const timeValue = avgTime[index] ?? 0;
          return `
            <strong>${labels[index] || ''}</strong><br/>
            Успешно: ${map['Успешно'] ?? 0}<br/>
            Ошибки: ${map['Нужно доработать'] ?? 0}<br/>
            Ср. балл: ${map['Средний балл'] ?? 0}<br/>
            Ср. время: ${timeValue} мин
          `;
        },
      },
      legend: {
        data: ['Успешно', 'Нужно доработать', 'Средний балл'],
        textStyle: { color: '#dbe7ff' },
      },
      grid: { left: 32, right: 32, top: 40, bottom: 40 },
      xAxis: {
        type: 'category',
        data: labels,
        axisLine: { lineStyle: { color: '#2f3f5f' } },
        axisLabel: { color: '#c7d2f1' },
      },
      yAxis: [
        {
          type: 'value',
          name: 'Переводы',
          axisLabel: { color: '#c7d2f1' },
          splitLine: { lineStyle: { color: 'rgba(255,255,255,0.08)' } },
        },
        {
          type: 'value',
          name: 'Баллы',
          min: 0,
          max: 100,
          axisLabel: { color: '#c7d2f1' },
          splitLine: { show: false },
        },
      ],
      series: [
        {
          name: 'Успешно',
          type: 'bar',
          stack: 'total',
          data: success,
          itemStyle: { color: '#06d6a0' },
          barWidth: 22,
        },
        {
          name: 'Нужно доработать',
          type: 'bar',
          stack: 'total',
          data: fail,
          itemStyle: { color: '#ff6b6b' },
          barWidth: 22,
        },
        {
          name: 'Средний балл',
          type: 'line',
          yAxisIndex: 1,
          data: avgScore,
          smooth: true,
          symbol: 'circle',
          symbolSize: 8,
          itemStyle: { color: '#ffd166' },
          lineStyle: { width: 3 },
        },
      ],
    });

    return () => {
      chart.dispose();
    };
  }, [analyticsPoints, analyticsPeriod]);

  useEffect(() => {
    if (!analyticsCompareRef.current) {
      return;
    }
    const chart = echarts.init(analyticsCompareRef.current);
    const selfId = webappUser?.id;
    const names = analyticsCompare.map((item) => item.username);
    const data = analyticsCompare.map((item) => ({
      value: item.final_score || 0,
      itemStyle: {
        color: item.user_id === selfId ? '#ffd166' : '#5ddcff',
      },
    }));

    chart.setOption({
      backgroundColor: 'transparent',
      tooltip: {
        trigger: 'item',
        formatter: (params) => {
          const item = analyticsCompare[params.dataIndex];
          if (!item) return '';
          return `
            <strong>${item.username}</strong><br/>
            Итоговый балл: ${item.final_score}<br/>
            Успех: ${item.success_rate}%<br/>
            Ср. балл: ${item.avg_score}<br/>
            Переводы: ${item.total_translations}<br/>
            Пропущено: ${item.missed_sentences}<br/>
            Пропущено дней: ${item.missed_days ?? 0}
          `;
        },
      },
      grid: { left: 20, right: 20, top: 20, bottom: 20, containLabel: true },
      xAxis: {
        type: 'value',
        axisLabel: { color: '#c7d2f1' },
        splitLine: { lineStyle: { color: 'rgba(255,255,255,0.08)' } },
      },
      yAxis: {
        type: 'category',
        data: names,
        axisLabel: { color: '#c7d2f1' },
        inverse: true,
      },
      series: [
        {
          type: 'bar',
          data,
          barWidth: 18,
          borderRadius: [8, 8, 8, 8],
        },
      ],
    });

    return () => {
      chart.dispose();
    };
  }, [analyticsCompare, webappUser]);

  if (isWebAppMode) {
    return (
      <div className={`webapp-page ${flashcardsOnly ? 'is-flashcards' : ''}`}>
        <div className="webapp-shell">
          <aside className="webapp-sidebar">
            <div className="webapp-brand">
              <div className="brand-mark">DF</div>
              <div>
                <div className="brand-title">DeutschFlow</div>
                <div className="brand-subtitle">Переводы • Видео • Словарь</div>
              </div>
            </div>
            <div className="webapp-menu">
              <button
                type="button"
                className={`menu-item ${selectedSections.has('translations') ? 'is-active' : ''}`}
                onClick={() => toggleSection('translations')}
                disabled={flashcardsOnly}
              >
                Переводы
              </button>
              <button
                type="button"
                className={`menu-item ${selectedSections.has('youtube') ? 'is-active' : ''}`}
                onClick={() => toggleSection('youtube')}
                disabled={flashcardsOnly}
              >
                YouTube
              </button>
              <button
                type="button"
                className={`menu-item ${selectedSections.has('dictionary') ? 'is-active' : ''}`}
                onClick={() => toggleSection('dictionary')}
                disabled={flashcardsOnly}
              >
                Словарь
              </button>
              <button
                type="button"
                className={`menu-item ${selectedSections.has('flashcards') ? 'is-active' : ''}`}
                onClick={() => {
                  toggleSection('flashcards');
                  setFlashcardsVisible(true);
                  setFlashcardsOnly(false);
                  setFlashcardSessionActive(false);
                  setFlashcardExitSummary(false);
                }}
              >
                Карточки
              </button>
              <button
                type="button"
                className={`menu-item ${selectedSections.has('analytics') ? 'is-active' : ''}`}
                onClick={() => toggleSection('analytics')}
                disabled={flashcardsOnly}
              >
                Аналитика
              </button>
            </div>
            <div className="webapp-menu-actions">
              <label className="menu-toggle-row">
                <input
                  type="checkbox"
                  checked={menuMultiSelect}
                  onChange={(event) => setMenuMultiSelect(event.target.checked)}
                />
                <span>Мультивыбор</span>
              </label>
              <button type="button" className="secondary-button" onClick={showAllSections} disabled={flashcardsOnly}>
                Показать все
              </button>
              <button type="button" className="secondary-button" onClick={hideAllSections} disabled={flashcardsOnly}>
                Скрыть всё
              </button>
            </div>
            {flashcardsOnly && (
              <div className="webapp-menu-note">Режим повторения активен</div>
            )}
          </aside>

          <div className="webapp-main">
            <div className="webapp-topbar">
              <button
                type="button"
                className="menu-toggle"
                onClick={() => setMenuOpen(true)}
              >
                <span />
                <span />
                <span />
              </button>
              <div className="topbar-title">DeutschFlow</div>
            </div>

            {menuOpen && (
              <div className="webapp-overlay">
                <div className="overlay-backdrop" onClick={() => setMenuOpen(false)} />
                <div className="overlay-panel">
                  <div className="overlay-header">
                    <div className="brand-title">DeutschFlow</div>
                    <button
                      type="button"
                      className="secondary-button"
                      onClick={() => setMenuOpen(false)}
                    >
                      Закрыть
                    </button>
                  </div>
                <div className="overlay-menu">
                    <label className="menu-toggle-row">
                      <input
                        type="checkbox"
                        checked={menuMultiSelect}
                        onChange={(event) => setMenuMultiSelect(event.target.checked)}
                      />
                      <span>Мультивыбор</span>
                    </label>
                    <button
                      type="button"
                      className={`menu-item ${selectedSections.has('translations') ? 'is-active' : ''}`}
                      onClick={() => handleMenuSelection('translations', translationsRef)}
                      disabled={flashcardsOnly}
                    >
                      Переводы
                    </button>
                    <button
                      type="button"
                      className={`menu-item ${selectedSections.has('youtube') ? 'is-active' : ''}`}
                      onClick={() => handleMenuSelection('youtube', youtubeRef)}
                      disabled={flashcardsOnly}
                    >
                      YouTube
                    </button>
                    <button
                      type="button"
                      className={`menu-item ${selectedSections.has('dictionary') ? 'is-active' : ''}`}
                      onClick={() => handleMenuSelection('dictionary', dictionaryRef)}
                      disabled={flashcardsOnly}
                    >
                      Словарь
                    </button>
                    <button
                      type="button"
                      className={`menu-item ${selectedSections.has('flashcards') ? 'is-active' : ''}`}
                      onClick={() => handleMenuSelection('flashcards', flashcardsRef)}
                    >
                      Карточки
                    </button>
                    <button
                      type="button"
                      className={`menu-item ${selectedSections.has('analytics') ? 'is-active' : ''}`}
                      onClick={() => handleMenuSelection('analytics', analyticsRef)}
                      disabled={flashcardsOnly}
                    >
                      Аналитика
                    </button>
                  </div>
                  <div className="overlay-actions">
                    <button type="button" className="secondary-button" onClick={showAllSections} disabled={flashcardsOnly}>
                      Показать все
                    </button>
                    <button type="button" className="secondary-button" onClick={hideAllSections} disabled={flashcardsOnly}>
                      Скрыть всё
                    </button>
                  </div>
                </div>
              </div>
            )}

            {showHero && (
            <header className="webapp-hero">
              <div className="webapp-hero-copy">
                <span className="pill">Telegram Web App</span>
                <h1>Учите немецкий в потоке</h1>
                <p>
                  Переводы, словарь, видео и карточки — всё в одном месте. Короткие шаги,
                  быстрые проверки и понятный прогресс без перегруза.
                </p>
              </div>
              <div className="webapp-user-badge">
                <input
                  ref={avatarInputRef}
                  type="file"
                  accept="image/*"
                  className="avatar-input"
                  onChange={handleAvatarUpload}
                />
                <button
                  type="button"
                  className="avatar-button"
                  onClick={() => avatarInputRef.current?.click()}
                >
                  {userAvatar ? <img src={userAvatar} alt="User avatar" /> : <span className="avatar-placeholder" />}
                </button>
                <div className="user-name">{webappUser?.first_name || 'Гость'}</div>
              </div>
            </header>
            )}

            {showHero && (
            <section className="webapp-hero-cards">
              <div className="hero-card">
                <h3>Переводите</h3>
                <p>Напишите перевод, получите оценку и объяснения ошибок.</p>
              </div>
              <div className="hero-card">
                <h3>Сохраняйте</h3>
                <p>Добавляйте слова в словарь и группируйте по папкам.</p>
              </div>
              <div className="hero-card">
                <h3>Тренируйтесь</h3>
                <p>Повторяйте слова сетами по 15 карточек с прогрессом.</p>
              </div>
            </section>
            )}

            {showHero && (
            <section className="webapp-quickstart">
              <div className="webapp-muted">Выберите раздел в меню, чтобы начать.</div>
            </section>
            )}

            

            {!telegramApp?.initData && (
              <label className="webapp-field">
                <span>initData (для локального теста)</span>
                <textarea
                  rows={3}
                  value={initData}
                  onChange={(event) => setInitData(event.target.value)}
                  placeholder="Вставьте initData из Telegram"
                />
              </label>
            )}

            {!flashcardsOnly && isSectionVisible('translations') && (
              <section className="webapp-section" ref={translationsRef}>
                <div className="webapp-section-title">
                  <h2>Ваши переводы</h2>
                </div>
                <div className="webapp-translation-start">
                  <label className="webapp-field">
                    <span>Тема</span>
                    <select
                      value={selectedTopic}
                      onChange={(event) => setSelectedTopic(event.target.value)}
                      disabled={topicsLoading || webappLoading}
                    >
                      {topics.map((topic) => (
                        <option key={topic} value={topic}>
                          {topic}
                        </option>
                      ))}
                    </select>
                  </label>
                  <button
                    type="button"
                    className="primary-button"
                    onClick={handleStartTranslation}
                    disabled={webappLoading || topicsLoading}
                  >
                    {webappLoading ? 'Запускаем...' : '🚀 Начать перевод'}
                  </button>
                </div>
                {topicsError && <div className="webapp-error">{topicsError}</div>}
                <form className="webapp-form" onSubmit={handleWebappSubmit}>
                  <section className="webapp-translation-list">
                    {sentences.length === 0 ? (
                      <p className="webapp-muted">
                        Нет активных предложений. Нажмите «🚀 Начать перевод», чтобы получить новые.
                      </p>
                    ) : (
                      sentences.map((item, index) => {
                        const draft = translationDrafts[String(item.id_for_mistake_table)] || '';
                        return (
                          <label key={item.id_for_mistake_table} className="webapp-translation-item">
                            <span>
                              {item.unique_id ?? index + 1}. {item.sentence}
                            </span>
                            <textarea
                              rows={5}
                              value={draft}
                              onChange={(event) => handleDraftChange(item.id_for_mistake_table, event.target.value)}
                              placeholder="Введите перевод..."
                            />
                          </label>
                        );
                      })
                    )}
                  </section>

                  <button className="primary-button" type="submit" disabled={webappLoading}>
                    {webappLoading ? 'Проверяем...' : 'Проверить перевод'}
                  </button>
                </form>

                {webappError && <div className="webapp-error">{webappError}</div>}
                {finishMessage && <div className="webapp-success">{finishMessage}</div>}

                {results.length > 0 && (
                  <section className="webapp-result">
                    <h3>Результат проверки</h3>
                    <div className="webapp-result-list">
                      {results.map((item, index) => (
                        <div key={`${item.sentence_number ?? index}`} className="webapp-result-card">
                          {item.error ? (
                            <div className="webapp-error">{item.error}</div>
                          ) : (
                            <>
                              <div
                                className="webapp-result-text"
                                onMouseUp={handleSelection}
                                onTouchEnd={handleSelection}
                              >
                                {renderFeedback(item.feedback)}
                              </div>
                              <button
                                type="button"
                                className="secondary-button explanation-button"
                                onClick={() => handleExplainTranslation(item)}
                                disabled={explanationLoading[String(item.sentence_number ?? item.original_text)]}
                              >
                                {explanationLoading[String(item.sentence_number ?? item.original_text)]
                                  ? 'Запрашиваем объяснение...'
                                  : 'Объяснить ошибки'}
                              </button>
                              {explanations[String(item.sentence_number ?? item.original_text)] && (
                                <div
                                  className="webapp-explanation"
                                  dangerouslySetInnerHTML={{
                                    __html: renderRichText(
                                      explanations[String(item.sentence_number ?? item.original_text)]
                                    ),
                                  }}
                                />
                              )}
                            </>
                          )}
                        </div>
                      ))}
                    </div>
                  </section>
                )}

                <div className="webapp-actions webapp-actions-footer">
                  <button
                    type="button"
                    onClick={handleFinishTranslation}
                    className={`primary-button finish-button ${finishStatus === 'done' ? 'status-done' : ''}`}
                    disabled={webappLoading || results.length === 0}
                  >
                    {finishStatus === 'done' ? 'Завершено' : 'Завершить перевод'}
                  </button>
                  <button
                    type="button"
                    onClick={handleLoadDailyHistory}
                    className="secondary-button"
                    disabled={webappLoading || historyLoading}
                  >
                    {historyLoading ? 'Загружаем...' : 'История за сегодня'}
                  </button>
                  {results.length === 0 && !webappLoading && (
                    <div className="webapp-muted">Сначала проверьте перевод, чтобы завершить.</div>
                  )}
                </div>

                {historyError && <div className="webapp-error">{historyError}</div>}

                {historyVisible && (
                  <section className="webapp-result">
                    <h3>История переводов за сегодня</h3>
                    {historyItems.length === 0 ? (
                      <p className="webapp-muted">Сегодня пока нет завершённых переводов.</p>
                    ) : (
                      <div className="webapp-result-list">
                        {historyItems.map((item, index) => (
                          <div key={item.id ?? index} className="webapp-result-card">
                            <pre className="webapp-result-text">
                              {`Sentence number: ${item.sentence_number ?? '—'}\nScore: ${
                                item.score ?? '—'
                              }/100\nOriginal: ${item.original_text ?? '—'}\nTranslation: ${
                                item.user_translation ?? '—'
                              }\nCorrect: ${item.correct_translation ?? '—'}`}
                            </pre>
                          </div>
                        ))}
                      </div>
                    )}
                  </section>
                )}
              </section>
            )}

            {!flashcardsOnly && (isSectionVisible('youtube') || isSectionVisible('dictionary')) && (
              <div className={`webapp-video-dictionary ${videoExpanded ? 'is-split' : ''}`}>
                {isSectionVisible('youtube') && (
                  <section className="webapp-video" ref={youtubeRef}>
                    <h3>Видео YouTube</h3>
                    <div className="webapp-video-form">
                      <label className="webapp-field">
                        <span>Ссылка или ID видео</span>
                        <input
                          type="text"
                          value={youtubeInput}
                          onChange={(event) => setYoutubeInput(event.target.value)}
                          placeholder="https://youtu.be/VIDEO_ID"
                        />
                      </label>
                      <div className="webapp-video-actions">
                        <button
                          type="button"
                          className="secondary-button"
                          onClick={() => setVideoExpanded((prev) => !prev)}
                        >
                          {videoExpanded ? 'Обычный режим' : 'Словарь рядом'}
                        </button>
                        {youtubeId && (
                          <a
                            className="secondary-button"
                            href={`https://www.youtube.com/watch?v=${youtubeId}`}
                            target="_blank"
                            rel="noopener noreferrer"
                          >
                            Открыть в YouTube
                          </a>
                        )}
                      </div>
                    </div>
                    {youtubeError && <div className="webapp-error">{youtubeError}</div>}
                    {youtubeId ? (
                      <div className={`webapp-video-frame ${videoExpanded ? 'is-expanded' : ''}`}>
                        <iframe
                          title="YouTube player"
                          src={`https://www.youtube.com/embed/${youtubeId}`}
                          allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
                          allowFullScreen
                        />
                      </div>
                    ) : (
                      <p className="webapp-muted">Вставьте ссылку на видео, чтобы смотреть прямо здесь.</p>
                    )}
                    <p className="webapp-muted">
                      Если видео не воспроизводится внутри Web App, используйте кнопку «Открыть в YouTube».
                    </p>

                    {/*
                    <div className="webapp-subtitles">
                      <div className="webapp-subtitles-header">
                        <h4>Субтитры</h4>
                        <div className="webapp-subtitles-actions">
                          <button
                            type="button"
                            className="secondary-button"
                            onClick={() => handleManualTranscript()}
                            disabled={!manualTranscript.trim()}
                          >
                            Использовать вставленные
                          </button>
                        </div>
                      </div>

                      {youtubeTranscriptLoading && <div className="webapp-muted">Загружаем субтитры...</div>}
                      {youtubeTranscriptError && <div className="webapp-error">{youtubeTranscriptError}</div>}

                      {youtubeTranscript.length > 0 ? (
                        <div className="webapp-subtitles-list" onMouseUp={handleSelection}>
                          {youtubeTranscript.map((item, index) => (
                            <p key={`${item.start}-${index}`}>
                              {renderClickableText(item.text)}
                            </p>
                          ))}
                        </div>
                      ) : (
                        <div className="webapp-subtitles-fallback">
                          <p className="webapp-muted">
                            Если авто-субтитры недоступны, вставьте текст субтитров ниже.
                          </p>
                          <textarea
                            rows={5}
                            value={manualTranscript}
                            onChange={(event) => setManualTranscript(event.target.value)}
                            placeholder="Вставьте .srt/.vtt или обычный текст субтитров"
                          />
                        </div>
                      )}
                    </div>
                    */}
                  </section>
                )}

                {isSectionVisible('dictionary') && (
                  <section className="webapp-dictionary" ref={dictionaryRef}>
                    <h3>Словарь</h3>
                    <form className="webapp-dictionary-form" onSubmit={handleDictionaryLookup}>
                      <label className="webapp-field">
                        <span>Слово или фраза (русский / немецкий)</span>
                        <input
                          className="dictionary-input"
                          type="text"
                          value={dictionaryWord}
                          onChange={(event) => setDictionaryWord(event.target.value)}
                          placeholder="Например: отказаться, уважение, несмотря на / verzichten, Respekt"
                        />
                      </label>
                      <div className="dictionary-actions">
                        <button className="secondary-button dictionary-button" type="submit" disabled={dictionaryLoading}>
                          {dictionaryLoading ? 'Ищем...' : 'Перевести'}
                        </button>
                        <button
                          className="secondary-button dictionary-save-button"
                          type="button"
                          onClick={handleDictionarySave}
                          disabled={dictionaryLoading || !dictionaryResult}
                        >
                          Добавить в словарь
                        </button>
                      </div>
                    </form>

                    <div className="folder-panel">
                      <div className="folder-row">
                        <label className="webapp-field folder-select">
                          <span>Папка для сохранения</span>
                          <select
                            value={dictionaryFolderId}
                            onChange={(event) => setDictionaryFolderId(event.target.value)}
                          >
                            <option value="none">Без папки</option>
                            {folders.map((folder) => (
                              <option key={folder.id} value={folder.id}>
                                {resolveFolderIconLabel(folder.icon)} • {folder.name}
                              </option>
                            ))}
                          </select>
                        </label>
                        {dictionaryFolderId !== 'none' && selectedDictionaryFolder && (
                          <div className="folder-preview">
                            {renderFolderIcon(
                              selectedDictionaryFolder.icon,
                              selectedDictionaryFolder.color
                            )}
                            <span>{selectedDictionaryFolder.name}</span>
                          </div>
                        )}
                        <button
                          type="button"
                          className="secondary-button"
                          onClick={() => setShowNewFolderForm((prev) => !prev)}
                        >
                          Новая папка
                        </button>
                        <button
                          type="button"
                          className="secondary-button"
                          onClick={handleExportDictionaryPdf}
                          disabled={exportLoading}
                        >
                          {exportLoading ? 'Готовим PDF...' : 'Выгрузить PDF'}
                        </button>
                      </div>
                      {showNewFolderForm && (
                        <div className="folder-create">
                          <label className="webapp-field">
                            <span>Название</span>
                            <input
                              type="text"
                              value={newFolderName}
                              onChange={(event) => setNewFolderName(event.target.value)}
                              placeholder="Например: Путешествия"
                            />
                          </label>
                          <div className="folder-pickers">
                            <div className="folder-picker">
                              <span>Цвет</span>
                              <div className="folder-color-options">
                                {folderColorOptions.map((color) => (
                                  <button
                                    key={color}
                                    type="button"
                                    className={`color-dot ${newFolderColor === color ? 'is-active' : ''}`}
                                    style={{ background: color }}
                                    onClick={() => setNewFolderColor(color)}
                                  />
                                ))}
                              </div>
                            </div>
                            <div className="folder-picker">
                              <span>Иконка</span>
                              <div className="folder-icon-options">
                                {folderIconOptions.map((icon) => (
                                  <button
                                    key={icon}
                                    type="button"
                                    className={`icon-dot ${newFolderIcon === icon ? 'is-active' : ''}`}
                                    onClick={() => setNewFolderIcon(icon)}
                                  >
                                    {renderFolderIcon(icon, newFolderColor)}
                                  </button>
                                ))}
                              </div>
                            </div>
                          </div>
                          <button
                            type="button"
                            className="primary-button"
                            onClick={handleCreateFolder}
                            disabled={foldersLoading}
                          >
                            {foldersLoading ? 'Создаём...' : 'Создать папку'}
                          </button>
                          {foldersError && <div className="webapp-error">{foldersError}</div>}
                        </div>
                      )}
                      {!showNewFolderForm && foldersError && <div className="webapp-error">{foldersError}</div>}
                    </div>

                    {dictionaryError && <div className="webapp-error">{dictionaryError}</div>}
                    {dictionarySaved && <div className="webapp-success">{dictionarySaved}</div>}

                    {dictionaryResult && (
                      <div className="webapp-dictionary-result">
                        {lastLookupScrollY !== null && (
                          <button
                            type="button"
                            className="dictionary-back-button"
                            onClick={() => window.scrollTo({ top: lastLookupScrollY, behavior: 'smooth' })}
                          >
                            ← вернуться назад
                          </button>
                        )}
                        <div className="dictionary-row">
                          <strong>Перевод:</strong>{' '}
                          {dictionaryDirection === 'ru-de'
                            ? (dictionaryResult.translation_de || '—')
                            : (dictionaryResult.translation_ru || '—')}
                          <span className="dictionary-direction">
                            {dictionaryDirection === 'ru-de' ? 'RU → DE' : 'DE → RU'}
                          </span>
                        </div>
                        <div className="dictionary-row">
                          <strong>Часть речи:</strong> {dictionaryResult.part_of_speech || '—'}
                        </div>
                        {dictionaryResult.article && (
                          <div className="dictionary-row">
                            <strong>Артикль:</strong> {dictionaryResult.article}
                          </div>
                        )}
                        {dictionaryResult.forms && (
                          <div className="dictionary-forms">
                            <div><strong>Plural:</strong> {dictionaryResult.forms.plural || '—'}</div>
                            <div><strong>Präteritum:</strong> {dictionaryResult.forms.praeteritum || '—'}</div>
                            <div><strong>Perfekt:</strong> {dictionaryResult.forms.perfekt || '—'}</div>
                            <div><strong>Konjunktiv I:</strong> {dictionaryResult.forms.konjunktiv1 || '—'}</div>
                            <div><strong>Konjunktiv II:</strong> {dictionaryResult.forms.konjunktiv2 || '—'}</div>
                          </div>
                        )}

                        {Array.isArray(dictionaryResult.prefixes) && dictionaryResult.prefixes.length > 0 && (
                          <div className="dictionary-prefixes">
                            <strong>Префиксы/варианты:</strong>
                            <ul>
                              {dictionaryResult.prefixes.map((item, index) => (
                                <li key={`${item.variant}-${index}`}>
                                  <div><strong>{item.variant}:</strong> {item.translation_de || '—'}</div>
                                  {item.explanation && <div>{item.explanation}</div>}
                                  {item.example_de && <div><em>{item.example_de}</em></div>}
                                </li>
                              ))}
                            </ul>
                          </div>
                        )}

                        {Array.isArray(dictionaryResult.usage_examples) && dictionaryResult.usage_examples.length > 0 && (
                          <div className="dictionary-examples">
                            <strong>Примеры:</strong>
                            <ul>
                              {dictionaryResult.usage_examples.map((example, index) => (
                                <li key={`${index}-${example}`}>{example}</li>
                              ))}
                            </ul>
                          </div>
                        )}
                      </div>
                    )}
                    {collocationsVisible && (
                      <div className="dictionary-collocations">
                        <h4>Выберите связку для словаря</h4>
                        {collocationsLoading && <div className="webapp-muted">Генерируем варианты...</div>}
                        {collocationsError && <div className="webapp-error">{collocationsError}</div>}
                        {!collocationsLoading && collocationOptions.length > 0 && (
                          <div className="collocation-list">
                            {collocationOptions.map((option, index) => (
                              <label key={`${option.source}-${index}`} className="collocation-item">
                                <input
                                  type="radio"
                                  name="collocation"
                                  checked={selectedCollocation?.source === option.source && selectedCollocation?.target === option.target}
                                  onChange={() => setSelectedCollocation(option)}
                                />
                                <div>
                                  <div className="collocation-source">{option.source}</div>
                                  <div className="collocation-target">{option.target}</div>
                                  {option.isBase && <span className="collocation-tag">Исходное</span>}
                                </div>
                              </label>
                            ))}
                          </div>
                        )}
                        <div className="collocation-actions">
                          <button
                            type="button"
                            className="primary-button"
                            onClick={handleConfirmSaveCollocation}
                            disabled={dictionaryLoading}
                          >
                            {dictionaryLoading ? 'Сохраняем...' : 'Добавить выбранное'}
                          </button>
                          <button
                            type="button"
                            className="secondary-button"
                            onClick={() => setCollocationsVisible(false)}
                          >
                            Отмена
                          </button>
                        </div>
                      </div>
                    )}
                    <div className="dictionary-flashcards">
                      <button
                        type="button"
                        className="secondary-button"
                        onClick={() => {
                          openFlashcardsSetup(flashcardsRef);
                        }}
                      >
                        Повторить слова
                      </button>
                    </div>
                  </section>
                )}
              </div>
            )}

            {isSectionVisible('flashcards') && (
              <section className="webapp-flashcards" ref={flashcardsRef}>
                {!flashcardsVisible && (
                  <div className="webapp-muted">
                    Нажмите «Повторить слова», чтобы начать тренировку.
                  </div>
                )}
                {flashcardsVisible && (
                  <div className={`flashcards-panel ${flashcardsOnly ? 'is-session' : 'is-setup'}`}>
                    {!flashcardsOnly && (
                      <div className="flashcard-stage is-setup">
                        <div className="flashcards-setup">
                          <div className="setup-hero">
                            <div className="setup-ring">
                              <img src="/mascot.svg" alt="DeutschFlow" className="setup-mascot" />
                            </div>
                            <div className="setup-title">Тренировка карточек</div>
                            <div className="setup-subtitle">Выберите параметры и стартуйте сет.</div>
                          </div>
                          <div className="setup-grid">
                            <div className="setup-group">
                              <div className="setup-label">Размер сета</div>
                              <div className="setup-options">
                                {[5, 10, 15].map((size) => (
                                  <button
                                    key={`set-${size}`}
                                    type="button"
                                    className={`option-pill ${flashcardSetSize === size ? 'is-active' : ''}`}
                                    onClick={() => setFlashcardSetSize(size)}
                                  >
                                    {size} карточек
                                  </button>
                                ))}
                              </div>
                            </div>
                            <div className="setup-group">
                              <div className="setup-label">Папка для тренировки</div>
                              <label className="webapp-field">
                                <select
                                  value={flashcardFolderMode === 'folder' ? flashcardFolderId : flashcardFolderMode}
                                  onChange={(event) => {
                                    const value = event.target.value;
                                    if (value === 'all') {
                                      setFlashcardFolderMode('all');
                                      setFlashcardFolderId('');
                                    } else if (value === 'none') {
                                      setFlashcardFolderMode('none');
                                      setFlashcardFolderId('');
                                    } else {
                                      setFlashcardFolderMode('folder');
                                      setFlashcardFolderId(value);
                                    }
                                  }}
                                >
                                  <option value="all">Все папки</option>
                                  <option value="none">Без папки</option>
                                  {folders.map((folder) => (
                                    <option key={folder.id} value={folder.id}>
                                      {resolveFolderIconLabel(folder.icon)} • {folder.name}
                                    </option>
                                  ))}
                                </select>
                              </label>
                            </div>
                            <div className="setup-group">
                              <div className="setup-label">Скорость</div>
                              <div className="setup-options">
                                {[5, 10, 15].map((seconds) => (
                                  <button
                                    key={`speed-${seconds}`}
                                    type="button"
                                    className={`option-pill ${flashcardDurationSec === seconds ? 'is-active' : ''}`}
                                    onClick={() => setFlashcardDurationSec(seconds)}
                                  >
                                    {seconds} сек
                                  </button>
                                ))}
                              </div>
                            </div>
                          </div>
                          <button
                            type="button"
                            className="primary-button flashcards-start"
                            onClick={() => {
                              unlockAudio();
                              setFlashcardTimerDurationSec(flashcardDurationSec);
                              setFlashcardSessionActive(true);
                              setFlashcardsOnly(true);
                              setFlashcardExitSummary(false);
                            }}
                          >
                            Начать тренировку
                          </button>
                        </div>
                      </div>
                    )}

                    {flashcardsOnly && (
                      <>
                        {flashcardsLoading && <div className="webapp-muted">Загружаем карточки...</div>}
                        {flashcardsError && <div className="webapp-error">{flashcardsError}</div>}
                        {!flashcardsLoading && !flashcardsError && flashcards.length === 0 && (
                          <div className="webapp-muted">Словарь пуст. Сначала добавьте слова.</div>
                        )}
                        {!flashcardsLoading && !flashcardsError && flashcards.length > 0 && (
                          <div className="flashcard-stage is-session">
                            {(flashcardSetComplete || flashcardExitSummary) ? (
                              <div className="flashcard-summary">
                                <h4>{flashcardSetComplete ? 'Сет завершён' : 'Повтор завершён'}</h4>
                                <div className="summary-grid">
                                  <div>
                                    <span>Итого слов</span>
                                    <strong>{flashcardStats.total}</strong>
                                  </div>
                                  <div>
                                    <span>Верно</span>
                                    <strong>{flashcardStats.correct}</strong>
                                  </div>
                                  <div>
                                    <span>Неверно</span>
                                    <strong>{flashcardStats.wrong}</strong>
                                  </div>
                                </div>
                                <div className="summary-actions">
                                  <button
                                    type="button"
                                    className="primary-button"
                                    onClick={loadFlashcards}
                                  >
                                    Да, следующий сет
                                  </button>
                                  <button
                                    type="button"
                                    className="secondary-button"
                                    onClick={() => {
                                      setFlashcardsVisible(false);
                                      setFlashcardsOnly(false);
                                      setFlashcardSessionActive(false);
                                      setFlashcardExitSummary(false);
                                    }}
                                  >
                                    Нет, завершить
                                  </button>
                                </div>
                              </div>
                            ) : (
                              (() => {
                                const entry = flashcards[flashcardIndex] || {};
                                const responseJson = entry.response_json || {};
                                const correct = entry.translation_de
                                  || responseJson.translation_de
                                  || entry.translation_ru
                                  || responseJson.translation_ru
                                  || '—';
                                const questionWord = entry.word_ru
                                  || responseJson.word_ru
                                  || entry.word_de
                                  || responseJson.word_de
                                  || '—';
                                const context = Array.isArray(responseJson.usage_examples)
                                  ? responseJson.usage_examples[0]
                                  : '';

                                return (
                                  <div className="flashcard">
                                    <div className="flashcard-header">
                                      <span className="flashcard-counter">
                                        {flashcardIndex + 1} / {flashcards.length}
                                      </span>
                                      <div className="flashcard-actions-row">
                                        <button
                                          type="button"
                                          className="flashcard-refresh"
                                          onClick={loadFlashcards}
                                        >
                                          Обновить
                                        </button>
                                      </div>
                                    </div>
                                    <div className="flashcard-hero">
                                      <div
                                        key={`timer-${flashcardTimerKey}`}
                                        className={`flashcard-timer ${flashcardSelection !== null ? 'is-paused' : ''}`}
                                        style={{ '--duration': `${flashcardTimerDurationSec}s` }}
                                      >
                                        <svg viewBox="0 0 120 120" aria-hidden="true">
                                          <circle className="timer-track" cx="60" cy="60" r="54" />
                                          <circle className="timer-progress" cx="60" cy="60" r="54" />
                                        </svg>
                                        <div className="flashcard-character">
                                          <img src="/mascot.svg" alt="Mascot" />
                                        </div>
                                      </div>
                                    </div>
                                    <div className="flashcard-word">{questionWord}</div>
                                    {flashcardSelection !== null && context && (
                                      <div className="flashcard-context flashcard-context-visible">{context}</div>
                                    )}
                                    <div className="flashcard-options">
                                      {flashcardOptions.map((option, idx) => {
                                        const isSelected = flashcardSelection === idx;
                                        const isCorrect = option === correct;
                                        const showResult = flashcardSelection !== null;
                                        const className = [
                                          'flashcard-option',
                                          showResult && isCorrect ? 'is-correct' : '',
                                          showResult && isSelected && !isCorrect ? 'is-wrong' : '',
                                        ]
                                          .filter(Boolean)
                                          .join(' ');
                                        return (
                                          <button
                                            key={`${option}-${idx}`}
                                            type="button"
                                            className={className}
                                            onClick={() => {
                                              if (flashcardSelection !== null) return;
                                              setFlashcardSelection(idx);
                                              setFlashcardTimedOut(false);
                                              unlockAudio();
                                              playFeedbackSound(option === correct ? 'positive' : 'negative');
                                              recordFlashcardAnswer(entry.id, option === correct);
                                              setFlashcardStats((prev) => ({
                                                ...prev,
                                                correct: prev.correct + (option === correct ? 1 : 0),
                                                wrong: prev.wrong + (option === correct ? 0 : 1),
                                              }));
                                              if (autoAdvanceTimeoutRef.current) {
                                                clearTimeout(autoAdvanceTimeoutRef.current);
                                                autoAdvanceTimeoutRef.current = null;
                                              }
                                              if (revealTimeoutRef.current) {
                                                clearTimeout(revealTimeoutRef.current);
                                              }
                                              revealTimeoutRef.current = setTimeout(() => {
                                                advanceFlashcard();
                                              }, 3000);
                                            }}
                                          >
                                            {option}
                                          </button>
                                        );
                                      })}
                                    </div>
                                    {flashcardTimedOut && (
                                      <div className="flashcard-timeout">die Zeit ist um</div>
                                    )}
                                    {flashcardSelection !== null && (
                                      <div className="flashcard-hint">
                                        Следующая карточка через 3 секунды
                                      </div>
                                    )}
                                  </div>
                                );
                              })()
                            )}
                          </div>
                        )}
                        {!(flashcardSetComplete || flashcardExitSummary) && (
                          <div className="flashcard-end">
                            <button
                              type="button"
                              className="secondary-button"
                              onClick={() => {
                                setFlashcardExitSummary(true);
                              }}
                            >
                              Закончить повтор
                            </button>
                          </div>
                        )}
                      </>
                    )}
                  </div>
                )}
              </section>
            )}

            {!flashcardsOnly && isSectionVisible('analytics') && (
              <section className="webapp-section webapp-analytics" ref={analyticsRef}>
                <div className="webapp-section-title">
                  <h2>Аналитика</h2>
                </div>
                <div className="analytics-controls">
                  <label className="webapp-field">
                    <span>Период</span>
                    <select
                      value={analyticsPeriod}
                      onChange={(event) => setAnalyticsPeriod(event.target.value)}
                    >
                      <option value="day">День</option>
                      <option value="week">Неделя</option>
                      <option value="month">Месяц</option>
                      <option value="quarter">Квартал</option>
                      <option value="half-year">Полугодие</option>
                      <option value="year">Год</option>
                      <option value="all">Все время</option>
                    </select>
                  </label>
                  <button
                    type="button"
                    className="secondary-button"
                    onClick={() => loadAnalytics()}
                    disabled={analyticsLoading}
                  >
                    {analyticsLoading ? 'Считаем...' : 'Обновить'}
                  </button>
                  {analyticsRank && (
                    <div className="analytics-rank">Ваше место: #{analyticsRank}</div>
                  )}
                </div>

                {analyticsError && <div className="webapp-error">{analyticsError}</div>}

                {analyticsSummary && (
                  <div className="analytics-cards">
                    <div className="analytics-card">
                      <span>Переводы</span>
                      <strong>{analyticsSummary.total_translations}</strong>
                    </div>
                    <div className="analytics-card">
                      <span>Успех</span>
                      <strong>{analyticsSummary.success_rate}%</strong>
                    </div>
                    <div className="analytics-card">
                      <span>Средний балл</span>
                      <strong>{analyticsSummary.avg_score}</strong>
                    </div>
                    <div className="analytics-card">
                      <span>Среднее время</span>
                      <strong>{analyticsSummary.avg_time_min} мин</strong>
                    </div>
                    <div className="analytics-card">
                      <span>Пропущено дней</span>
                      <strong>{analyticsSummary.missed_days}</strong>
                    </div>
                    <div className="analytics-card">
                      <span>Пропущено</span>
                      <strong>{analyticsSummary.missed_sentences}</strong>
                    </div>
                    <div className="analytics-card">
                      <span>Итоговый балл</span>
                      <strong>{analyticsSummary.final_score}</strong>
                    </div>
                  </div>
                )}

                <div className="analytics-chart" ref={analyticsTrendRef} />
                <div className="analytics-chart analytics-compare" ref={analyticsCompareRef} />
              </section>
            )}

            {selectionText && selectionPos && (isSectionVisible('youtube') || isSectionVisible('dictionary')) && (
              <div
                className="webapp-selection-menu"
                style={{ left: `${selectionPos.x + 8}px`, top: `${selectionPos.y + 8}px` }}
                onMouseLeave={clearSelection}
              >
                <div className="webapp-selection-text">{selectionText}</div>
                <button
                  type="button"
                  className="secondary-button"
                  onClick={() => handleQuickLookupDictionary(selectionText)}
                >
                  Перевести
                </button>
                <button
                  type="button"
                  className="secondary-button"
                  onClick={() => handleQuickAddToDictionary(selectionText)}
                >
                  Добавить в словарь
                </button>
              </div>
            )}
          </div>
        </div>
      </div>
    );
  }

  // Если токена еще нет, показываем форму для входа
  // <form>: Это HTML-тег для сбора данных. Его особенность: он умеет реагировать на нажатие клавиши Enter на клавиатуре.
  // Когда пользователь нажимает Enter, форма автоматически вызывает функцию, указанную в onSubmit.
  // В нашем случае это handleConnect.
  // Таким образом, пользователь может либо нажать кнопку "Войти",
  // либо просто нажать Enter после ввода имени, и форма все равно сработает.
  // onSubmit — это событие "Отправка формы" (когда нажали кнопку submit или Enter).
  // e.preventDefault() внутри handleConnect предотвращает стандартное поведение формы — перезагрузку страницы.
  // {handleConnect} — мы говорим: "Когда случится отправка, НЕ перезагружай страницу (как делают старые сайты), а запусти нашу функцию handleConnect".
  // <h2>: Header 2. Заголовок второго уровня (жирный, крупный текст). Просто надпись.
  // Поле ввода <input> (Связь с памятью):
  // Это самая сложная концепция React, называется "Управляемый компонент" (Controlled Component).
  // Идея в том, что значение поля ввода (input) "связывается" с состоянием React (переменная username).
  // Когда пользователь вводит текст, срабатывает событие onChange.
  // Мы ловим это событие и вызываем setUsername с новым значением e.target.value.
  // Это обновляет состояние username в React.
  // Поскольку состояние изменилось, React перерисовывает компонент App,
  // и новое значение username снова "попадает" в поле ввода через атрибут value={username}.
  // Таким образом, поле ввода всегда "отражает" текущее состояние username.
  // Итог: Поле ввода и состояние username "связаны" друг с другом.
  // Любое изменение в поле ввода обновляет состояние,
  // а любое изменение состояния обновляет отображаемое значение в поле ввода.
  // Это позволяет нам точно контролировать, что находится в поле ввода в любой момент времени.
  // Кнопка <button type="submit">: Кнопка для отправки формы. При нажатии запускается событие onSubmit формы, вызывая handleConnect.
if (!token) {
    return (
      <div className="lesson-page lesson-login" data-lk-theme="default">
        <div className="lesson-bg" aria-hidden="true" />
        <div className="login-card">
          <div className="login-header">
            <span className="pill">Deutsch Tutor</span>
            <h2>Вход в урок</h2>
            <p>Подключитесь к разговорной практике и начните диалог с учителем.</p>
          </div>
          <form onSubmit={handleConnect} className="login-form">
            <label className="field">
              <span>Telegram ID</span>
              <input
                type="text"
                placeholder="Ваш Telegram ID (цифры)"
                value={telegramID}
                onChange={(e) => setTelegramID(e.target.value)}
              />
            </label>

            <label className="field">
              <span>Ваше имя</span>
              <input
                type="text"
                placeholder="Как вас называть? (Имя)"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
              />
            </label>

            <button type="submit" className="primary-button">
              Начать урок
            </button>
          </form>
        </div>
      </div>
    );
  }

  return (
    <LiveKitRoom
      serverUrl={livekitUrl}
      token={token}
      connect={true}
      audio={true}
      video={false}
      onDisconnected={() => setToken(null)}
      onError={(e) => console.error("LiveKit error:", e)}
      className="lesson-page lesson-room"
      data-lk-theme="default"
    >
      <div className="lesson-bg" aria-hidden="true" />
      <div className="lesson-shell">
        <header className="lesson-header">
          <div>
            <span className="pill">Учитель онлайн</span>
            <h1>Живая практика немецкого</h1>
            <p>Говорите свободно, а помощник ведет диалог, исправляет и поддерживает.</p>
          </div>
          <div className="lesson-meta">
            <span>Пользователь: {username}</span>
            <span>ID: {telegramID}</span>
          </div>
        </header>

        <main className="lesson-main">
          <section className="lesson-hero">
            <div className="lesson-illustration" aria-hidden="true">
              <svg viewBox="0 0 320 320" role="img">
                <defs>
                  <linearGradient id="bookGlow" x1="0" y1="0" x2="1" y2="1">
                    <stop offset="0%" stopColor="#ffb347" />
                    <stop offset="100%" stopColor="#ff7e5f" />
                  </linearGradient>
                </defs>
                <circle cx="160" cy="160" r="120" fill="#fff1d6" />
                <path d="M95 110c0-12 10-22 22-22h58c12 0 22 10 22 22v100c0 8-6 15-14 16-20 2-44 2-66 0-12-1-22-10-22-22z" fill="url(#bookGlow)" />
                <path d="M185 88h32c12 0 22 10 22 22v100c0 12-10 22-22 22h-32" fill="#ffd7aa" />
                <path d="M120 135h60M120 165h60M120 195h50" stroke="#6b3a1a" strokeWidth="6" strokeLinecap="round" />
                <circle cx="210" cy="90" r="26" fill="#6b3a1a" />
                <path d="M198 86h24v8h-24zM210 72v32" fill="#fff1d6" />
              </svg>
            </div>
            <div className="lesson-copy">
              <h2>Сфокусируйтесь на голосе</h2>
              <p>Нажмите на микрофон, чтобы включить речь, и нажмите выход, когда урок завершен.</p>
              <div className="lesson-tips">
                <div className="tip">Четко формулируйте ответы, чтобы учитель слышал интонацию.</div>
                <div className="tip">Если нужно подумать, просто сделайте паузу — связь сохранится.</div>
              </div>
            </div>
          </section>

          <section className="lesson-controls">
            <h3>Управление уроком</h3>
            <p>Все основные действия собраны в центре: микрофон, выход и настройки.</p>
            <div className="lesson-control-bar">
              <ControlBar />
            </div>
            <div className="lesson-hint">Совет: держите окно открытым, чтобы учитель не прерывал сессию.</div>
          </section>
        </main>
      </div>

      <RoomAudioRenderer />
      <ConnectionStateToast />
    </LiveKitRoom>
  );
}

export default function App() {
  return (
    <ErrorBoundary>
      <AppInner />
    </ErrorBoundary>
  );
}
