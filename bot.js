require('dotenv').config();
const TelegramBot = require('node-telegram-bot-api');
const Database = require('better-sqlite3');
const axios = require('axios');
const path = require('path');
const fs = require('fs');

// ─── Config ───
const BOT_TOKEN = process.env.BOT_TOKEN;
const ADMIN_IDS = (process.env.ADMIN_IDS || '').split(',').map(id => parseInt(id.trim())).filter(Boolean);

// ─── Database ───
const dataDir = path.join(__dirname, 'data');
if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });

const db = new Database(path.join(dataDir, 'bot.db'));
db.pragma('journal_mode = WAL');

db.exec(`
  CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    last_active TEXT DEFAULT (datetime('now')),
    total_requests INTEGER DEFAULT 0
  );
  CREATE TABLE IF NOT EXISTS requests_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    request_type TEXT,
    query TEXT,
    success INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now'))
  );
`);

const stmtUpsertUser = db.prepare(`
  INSERT INTO users (user_id, username, first_name, last_name)
  VALUES (?, ?, ?, ?)
  ON CONFLICT(user_id) DO UPDATE SET
    username = excluded.username,
    first_name = excluded.first_name,
    last_name = excluded.last_name,
    last_active = datetime('now'),
    total_requests = total_requests + 1
`);

const stmtLogRequest = db.prepare(`
  INSERT INTO requests_log (user_id, request_type, query, success)
  VALUES (?, ?, ?, ?)
`);

const stmtGetStats = db.prepare(`SELECT COUNT(*) as cnt FROM users`);
const stmtGetTotalRequests = db.prepare(`SELECT COUNT(*) as cnt FROM requests_log`);
const stmtGetUser = db.prepare(`SELECT * FROM users WHERE user_id = ?`);

// ─── Bot Init ───
const bot = new TelegramBot(BOT_TOKEN, { polling: true });

// ─── Emoji IDs ───
const E = {
  settings: '5870982283724328568',
  profile: '5870994129244131212',
  people: '5870772616305839506',
  verified: '5891207662678317861',
  unverified: '5893192487324880883',
  file: '5870528606328852614',
  smile: '5870764288364252592',
  growth: '5870930636742595124',
  stats: '5870921681735781843',
  home: '5873147866364514353',
  lock_closed: '6037249452824072506',
  lock_open: '6037496202990194718',
  megaphone: '6039422865189638057',
  check: '5870633910337015697',
  cross: '5870657884844462243',
  pencil: '5870676941614354370',
  trash: '5870875489362513438',
  down_arrow: '5893057118545646106',
  clip: '6039451237743595514',
  link: '5769289093221454192',
  info: '6028435952299413210',
  bot_emoji: '6030400221232501136',
  eye: '6037397706505195857',
  hidden: '6037243349675544634',
  send: '5963103826075456248',
  download: '6039802767931871481',
  bell: '6039486778597970865',
  gift: '6032644646587338669',
  clock: '5983150113483134607',
  party: '6041731551845159060',
  font: '5870801517140775623',
  write: '5870753782874246579',
  media: '6035128606563241721',
  geo: '6042011682497106307',
  wallet: '5769126056262898415',
  box: '5884479287171485878',
  calendar: '5890937706803894250',
  tag: '5886285355279193209',
  time_passed: '5775896410780079073',
  apps: '5778672437122045013',
  brush: '6050679691004612757',
  add_text: '5771851822897566479',
  resolution: '5778479949572738874',
  money: '5904462880941545555',
  code: '5940433880585605708',
  loading: '5345906554510012647',
  back: '5368324170671202286',
};

// ─── Helper: Premium emoji in text ───
function e(emojiId, fallback = '') {
  return `<tg-emoji emoji-id="${emojiId}">${fallback || '▪️'}</tg-emoji>`;
}

// ─── Helper: Format numbers ───
function formatNum(n) {
  if (n === null || n === undefined) return '—';
  const num = parseInt(n);
  if (isNaN(num)) return '—';
  if (num >= 1_000_000_000) return (num / 1_000_000_000).toFixed(1) + 'B';
  if (num >= 1_000_000) return (num / 1_000_000).toFixed(1) + 'M';
  if (num >= 1_000) return (num / 1_000).toFixed(1) + 'K';
  return num.toLocaleString('ru-RU');
}

// ─── Helper: Format date ───
function formatDate(ts) {
  if (!ts) return '—';
  const d = new Date(ts * 1000);
  if (isNaN(d.getTime())) return '—';
  return d.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: 'numeric' });
}

// ─── Helper: Extract TikTok username from input ───
function extractUsername(input) {
  input = input.trim();
  // URL patterns
  const urlMatch = input.match(/tiktok\.com\/@([a-zA-Z0-9_.]+)/);
  if (urlMatch) return urlMatch[1];
  // @username
  if (input.startsWith('@')) return input.slice(1);
  // plain username
  if (/^[a-zA-Z0-9_.]+$/.test(input)) return input;
  return null;
}

// ─── Helper: Extract TikTok video ID from URL ───
function extractVideoId(input) {
  input = input.trim();
  // Standard video URL
  const match = input.match(/tiktok\.com\/@[^/]+\/video\/(\d+)/);
  if (match) return match[1];
  // Short URL with video ID
  const match2 = input.match(/\/video\/(\d+)/);
  if (match2) return match2[1];
  // Just a numeric ID
  if (/^\d{15,25}$/.test(input)) return input;
  // vm.tiktok.com short links — we'll handle via redirect
  if (input.includes('vm.tiktok.com') || input.includes('vt.tiktok.com')) return input;
  return null;
}

// ─── TikTok API: Fetch Profile ───
async function fetchTikTokProfile(username) {
  try {
    // Primary: TikTok unofficial API via RapidAPI-style endpoint
    // We'll use a public scraping approach
    const response = await axios.get(`https://www.tiktok.com/@${username}`, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
      },
      timeout: 15000,
      maxRedirects: 5,
    });

    const html = response.data;

    // Extract SIGI_STATE or __UNIVERSAL_DATA_FOR_REHYDRATION__
    let userData = null;

    // Method 1: __UNIVERSAL_DATA_FOR_REHYDRATION__
    const universalMatch = html.match(/<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>([\s\S]*?)<\/script>/);
    if (universalMatch) {
      try {
        const jsonData = JSON.parse(universalMatch[1]);
        const defaultScope = jsonData?.['__DEFAULT_SCOPE__'];
        const userDetail = defaultScope?.['webapp.user-detail'];
        if (userDetail?.userInfo) {
          userData = userDetail.userInfo;
        }
      } catch (e) { /* ignore parse error */ }
    }

    // Method 2: SIGI_STATE
    if (!userData) {
      const sigiMatch = html.match(/<script id="SIGI_STATE"[^>]*>([\s\S]*?)<\/script>/);
      if (sigiMatch) {
        try {
          const sigiData = JSON.parse(sigiMatch[1]);
          const userModule = sigiData?.UserModule;
          if (userModule?.users?.[username]) {
            userData = {
              user: userModule.users[username],
              stats: userModule.stats?.[username],
            };
          }
        } catch (e) { /* ignore */ }
      }
    }

    // Method 3: JSON-LD or og:tags fallback
    if (!userData) {
      const jsonLdMatch = html.match(/<script type="application\/ld\+json"[^>]*>([\s\S]*?)<\/script>/);
      if (jsonLdMatch) {
        try {
          const ld = JSON.parse(jsonLdMatch[1]);
          if (ld?.['@type'] === 'Person' || ld?.name) {
            userData = { _fromLD: true, ld };
          }
        } catch (e) { /* ignore */ }
      }
    }

    if (!userData) return null;

    // Normalize data
    const user = userData.user || {};
    const stats = userData.stats || {};

    return {
      username: user.uniqueId || username,
      nickname: user.nickname || user.uniqueId || username,
      bio: user.signature || '',
      verified: user.verified || false,
      privateAccount: user.privateAccount || false,
      avatar: user.avatarLarger || user.avatarMedium || user.avatarThumb || '',
      followers: stats.followerCount ?? user.followerCount ?? null,
      following: stats.followingCount ?? user.followingCount ?? null,
      likes: stats.heartCount ?? stats.heart ?? user.heartCount ?? null,
      videos: stats.videoCount ?? user.videoCount ?? null,
      friends: stats.friendCount ?? user.friendCount ?? null,
      diggCount: stats.diggCount ?? user.diggCount ?? null,
      createTime: user.createTime || null,
      region: user.region || null,
      language: user.language || null,
      openFavorite: user.openFavorite || false,
      relation: user.relation || null,
      commentSetting: user.commentSetting,
      duetSetting: user.duetSetting,
      stitchSetting: user.stitchSetting,
      isUnderAge18: user.isUnderAge18 || false,
      secUid: user.secUid || '',
      id: user.id || '',
    };
  } catch (err) {
    console.error('Profile fetch error:', err.message);
    return null;
  }
}

// ─── TikTok API: Fetch Video Stats ───
async function fetchTikTokVideo(videoInput) {
  try {
    let videoUrl = videoInput;

    // If it's a short URL, resolve it first
    if (videoInput.includes('vm.tiktok.com') || videoInput.includes('vt.tiktok.com')) {
      if (!videoInput.startsWith('http')) videoUrl = 'https://' + videoInput;
      try {
        const headRes = await axios.head(videoUrl, {
          maxRedirects: 10,
          timeout: 10000,
          headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
          },
        });
        videoUrl = headRes.request?.res?.responseUrl || headRes.headers?.location || videoUrl;
      } catch (redirectErr) {
        if (redirectErr.response?.headers?.location) {
          videoUrl = redirectErr.response.headers.location;
        } else if (redirectErr.request?._currentUrl) {
          videoUrl = redirectErr.request._currentUrl;
        }
      }
    }

    // Extract video ID from resolved URL
    const vidMatch = videoUrl.match(/\/video\/(\d+)/);
    const videoId = vidMatch ? vidMatch[1] : videoInput;

    // Extract username from URL
    const userMatch = videoUrl.match(/tiktok\.com\/@([a-zA-Z0-9_.]+)/);
    const urlUsername = userMatch ? userMatch[1] : '';

    // Fetch the video page
    const fetchUrl = urlUsername
      ? `https://www.tiktok.com/@${urlUsername}/video/${videoId}`
      : `https://www.tiktok.com/video/${videoId}`;

    const response = await axios.get(fetchUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
      },
      timeout: 15000,
      maxRedirects: 5,
    });

    const html = response.data;
    let videoData = null;

    // Method 1: __UNIVERSAL_DATA_FOR_REHYDRATION__
    const universalMatch = html.match(/<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>([\s\S]*?)<\/script>/);
    if (universalMatch) {
      try {
        const jsonData = JSON.parse(universalMatch[1]);
        const defaultScope = jsonData?.['__DEFAULT_SCOPE__'];
        const videoDetail = defaultScope?.['webapp.video-detail'];
        if (videoDetail?.itemInfo?.itemStruct) {
          videoData = videoDetail.itemInfo.itemStruct;
        }
      } catch (e) { /* ignore */ }
    }

    // Method 2: SIGI_STATE
    if (!videoData) {
      const sigiMatch = html.match(/<script id="SIGI_STATE"[^>]*>([\s\S]*?)<\/script>/);
      if (sigiMatch) {
        try {
          const sigiData = JSON.parse(sigiMatch[1]);
          const itemModule = sigiData?.ItemModule;
          if (itemModule) {
            const key = Object.keys(itemModule)[0];
            if (key) videoData = itemModule[key];
          }
        } catch (e) { /* ignore */ }
      }
    }

    if (!videoData) return null;

    const stats = videoData.stats || {};
    const video = videoData.video || {};
    const music = videoData.music || {};
    const author = videoData.author || {};
    const challenges = videoData.challenges || [];
    const textExtra = videoData.textExtra || [];

    return {
      id: videoData.id || videoId,
      desc: videoData.desc || '',
      createTime: videoData.createTime || null,
      // Stats
      plays: stats.playCount ?? null,
      likes: stats.diggCount ?? null,
      comments: stats.commentCount ?? null,
      shares: stats.shareCount ?? null,
      saves: stats.collectCount ?? stats.bookmarkCount ?? null,
      reposts: stats.repostCount ?? null,
      // Video info
      duration: video.duration || null,
      ratio: video.ratio || null,
      format: video.format || null,
      cover: video.cover || video.originCover || '',
      // Music
      musicTitle: music.title || '',
      musicAuthor: music.authorName || '',
      musicOriginal: music.original || false,
      musicDuration: music.duration || null,
      musicAlbum: music.album || '',
      // Author
      authorUsername: author.uniqueId || videoData.author?.uniqueId || urlUsername || '',
      authorNickname: author.nickname || '',
      authorVerified: author.verified || false,
      authorAvatar: author.avatarThumb || '',
      // Tags
      hashtags: challenges.map(c => c.title).filter(Boolean),
      mentions: textExtra.filter(t => t.userUniqueId).map(t => t.userUniqueId),
      // Engagement
      diversificationLabels: videoData.diversificationLabels || [],
      isAd: videoData.isAd || false,
      isPinnedItem: videoData.isPinnedItem || false,
      duetEnabled: videoData.duetEnabled ?? null,
      stitchEnabled: videoData.stitchEnabled ?? null,
    };
  } catch (err) {
    console.error('Video fetch error:', err.message);
    return null;
  }
}

// ─── User state management ───
const userStates = new Map();

function setState(userId, state, data = {}) {
  userStates.set(userId, { state, data, ts: Date.now() });
}

function getState(userId) {
  return userStates.get(userId) || null;
}

function clearState(userId) {
  userStates.delete(userId);
}

// ─── Keyboards ───
function mainMenuKeyboard() {
  return {
    inline_keyboard: [
      [
        {
          text: 'Профиль',
          callback_data: 'menu_profile',
          icon_custom_emoji_id: E.profile,
        },
        {
          text: 'Видео',
          callback_data: 'menu_video',
          icon_custom_emoji_id: E.media,
        },
      ],
      [
        {
          text: 'Мои запросы',
          callback_data: 'menu_my_stats',
          icon_custom_emoji_id: E.stats,
        },
        {
          text: 'Информация',
          callback_data: 'menu_info',
          icon_custom_emoji_id: E.info,
        },
      ],
    ],
  };
}

function backToMenuKeyboard() {
  return {
    inline_keyboard: [
      [
        {
          text: 'Назад',
          callback_data: 'back_menu',
          icon_custom_emoji_id: E.back,
        },
      ],
    ],
  };
}

function profileResultKeyboard(username) {
  return {
    inline_keyboard: [
      [
        {
          text: 'Открыть TikTok',
          url: `https://www.tiktok.com/@${username}`,
          icon_custom_emoji_id: E.link,
        },
      ],
      [
        {
          text: 'Новый запрос',
          callback_data: 'menu_profile',
          icon_custom_emoji_id: E.profile,
        },
        {
          text: 'Меню',
          callback_data: 'back_menu',
          icon_custom_emoji_id: E.home,
        },
      ],
    ],
  };
}

function videoResultKeyboard(videoId, authorUsername) {
  const buttons = [];
  if (authorUsername) {
    buttons.push([
      {
        text: 'Открыть видео',
        url: `https://www.tiktok.com/@${authorUsername}/video/${videoId}`,
        icon_custom_emoji_id: E.link,
      },
    ]);
  }
  buttons.push([
    {
      text: 'Новый запрос',
      callback_data: 'menu_video',
      icon_custom_emoji_id: E.media,
    },
    {
      text: 'Меню',
      callback_data: 'back_menu',
      icon_custom_emoji_id: E.home,
    },
  ]);
  return { inline_keyboard: buttons };
}

function cancelKeyboard() {
  return {
    inline_keyboard: [
      [
        {
          text: 'Отмена',
          callback_data: 'back_menu',
          icon_custom_emoji_id: E.cross,
        },
      ],
    ],
  };
}

function adminKeyboard() {
  return {
    inline_keyboard: [
      [
        {
          text: 'Статистика бота',
          callback_data: 'admin_stats',
          icon_custom_emoji_id: E.stats,
        },
      ],
      [
        {
          text: 'Назад',
          callback_data: 'back_menu',
          icon_custom_emoji_id: E.back,
        },
      ],
    ],
  };
}

// ─── Message builders ───
function buildMainMenuText() {
  return [
    `${e(E.home, '🏠')} <b>TikTok Stats Bot</b>`,
    ``,
    `${e(E.profile, '👤')} <b>Профиль</b> — статистика аккаунта`,
    `${e(E.media, '🖼')} <b>Видео</b> — статистика видео`,
    ``,
    `${e(E.info, 'ℹ')} Выберите действие:`,
  ].join('\n');
}

function buildProfileText(p) {
  const verifiedIcon = p.verified ? e(E.verified, '✅') : e(E.unverified, '❌');
  const privateIcon = p.privateAccount ? e(E.lock_closed, '🔒') : e(E.lock_open, '🔓');

  const lines = [
    `${e(E.profile, '👤')} <b>Профиль: @${escHtml(p.username)}</b>`,
    ``,
    `${e(E.tag, '🏷')} <b>Имя:</b> ${escHtml(p.nickname)}`,
    `${verifiedIcon} <b>Верификация:</b> ${p.verified ? 'Да' : 'Нет'}`,
    `${privateIcon} <b>Приватный:</b> ${p.privateAccount ? 'Да' : 'Нет'}`,
  ];

  if (p.bio) {
    lines.push(`${e(E.pencil, '✏')} <b>Био:</b> ${escHtml(p.bio.substring(0, 200))}`);
  }

  lines.push(``);
  lines.push(`${e(E.stats, '📊')} <b>Статистика:</b>`);
  lines.push(`${e(E.people, '👥')} Подписчики: <b>${formatNum(p.followers)}</b>`);
  lines.push(`${e(E.eye, '👁')} Подписки: <b>${formatNum(p.following)}</b>`);

  if (p.likes !== null) {
    lines.push(`${e(E.growth, '❤')} Лайки: <b>${formatNum(p.likes)}</b>`);
  }
  if (p.videos !== null) {
    lines.push(`${e(E.media, '🎬')} Видео: <b>${formatNum(p.videos)}</b>`);
  }
  if (p.friends !== null && p.friends > 0) {
    lines.push(`${e(E.smile, '🙂')} Друзья: <b>${formatNum(p.friends)}</b>`);
  }
  if (p.diggCount !== null && p.diggCount > 0) {
    lines.push(`${e(E.check, '👍')} Лайкнул: <b>${formatNum(p.diggCount)}</b>`);
  }

  // Additional info
  const extras = [];
  if (p.region) extras.push(`${e(E.geo, '📍')} Регион: <b>${escHtml(p.region)}</b>`);
  if (p.language) extras.push(`${e(E.font, '🔤')} Язык: <b>${escHtml(p.language)}</b>`);
  if (p.createTime) extras.push(`${e(E.calendar, '📅')} Создан: <b>${formatDate(p.createTime)}</b>`);
  if (p.id) extras.push(`${e(E.code, '🔨')} ID: <code>${p.id}</code>`);

  if (extras.length > 0) {
    lines.push(``);
    lines.push(`${e(E.settings, '⚙')} <b>Доп. информация:</b>`);
    lines.push(...extras);
  }

  // Settings
  const settings = [];
  if (p.openFavorite) settings.push('Избранное открыто');
  if (p.commentSetting === 0) settings.push('Комментарии: все');
  else if (p.commentSetting === 1) settings.push('Комментарии: друзья');
  else if (p.commentSetting === 2) settings.push('Комментарии: выкл');

  if (settings.length > 0) {
    lines.push(`${e(E.settings, '⚙')} ${settings.join(' • ')}`);
  }

  return lines.join('\n');
}

function buildVideoText(v) {
  const lines = [
    `${e(E.media, '🎬')} <b>Видео от @${escHtml(v.authorUsername)}</b>`,
    ``,
  ];

  if (v.desc) {
    const shortDesc = v.desc.length > 150 ? v.desc.substring(0, 150) + '…' : v.desc;
    lines.push(`${e(E.pencil, '📝')} ${escHtml(shortDesc)}`);
    lines.push(``);
  }

  lines.push(`${e(E.stats, '📊')} <b>Статистика:</b>`);
  if (v.plays !== null) lines.push(`${e(E.eye, '👁')} Просмотры: <b>${formatNum(v.plays)}</b>`);
  if (v.likes !== null) lines.push(`${e(E.growth, '❤')} Лайки: <b>${formatNum(v.likes)}</b>`);
  if (v.comments !== null) lines.push(`${e(E.write, '💬')} Комментарии: <b>${formatNum(v.comments)}</b>`);
  if (v.shares !== null) lines.push(`${e(E.send, '↗')} Репосты: <b>${formatNum(v.shares)}</b>`);
  if (v.saves !== null) lines.push(`${e(E.download, '💾')} Сохранения: <b>${formatNum(v.saves)}</b>`);
  if (v.reposts !== null && v.reposts > 0) lines.push(`${e(E.megaphone, '📣')} Репосты TT: <b>${formatNum(v.reposts)}</b>`);

  // Engagement rate
  if (v.plays && v.plays > 0 && v.likes !== null) {
    const er = ((v.likes + (v.comments || 0) + (v.shares || 0)) / v.plays * 100).toFixed(2);
    lines.push(`${e(E.growth, '📈')} ER: <b>${er}%</b>`);
  }

  lines.push(``);
  lines.push(`${e(E.settings, '⚙')} <b>Информация:</b>`);

  if (v.duration) {
    const mins = Math.floor(v.duration / 60);
    const secs = v.duration % 60;
    lines.push(`${e(E.clock, '⏱')} Длительность: <b>${mins > 0 ? mins + 'м ' : ''}${secs}с</b>`);
  }

  if (v.createTime) {
    lines.push(`${e(E.calendar, '📅')} Дата: <b>${formatDate(v.createTime)}</b>`);
  }

  if (v.musicTitle) {
    const musicInfo = `${escHtml(v.musicTitle)}${v.musicAuthor ? ' — ' + escHtml(v.musicAuthor) : ''}`;
    lines.push(`${e(E.bell, '🎵')} Музыка: <b>${musicInfo}</b>${v.musicOriginal ? ' (оригинал)' : ''}`);
  }

  if (v.ratio) {
    lines.push(`${e(E.resolution, '📐')} Качество: <b>${v.ratio}</b>`);
  }

  if (v.hashtags && v.hashtags.length > 0) {
    const tags = v.hashtags.slice(0, 8).map(t => `#${t}`).join(' ');
    lines.push(`${e(E.tag, '🏷')} Теги: ${escHtml(tags)}`);
  }

  if (v.mentions && v.mentions.length > 0) {
    const ments = v.mentions.slice(0, 5).map(m => `@${m}`).join(' ');
    lines.push(`${e(E.people, '👥')} Упоминания: ${escHtml(ments)}`);
  }

  // Flags
  const flags = [];
  if (v.isAd) flags.push('Реклама');
  if (v.isPinnedItem) flags.push('Закреплено');
  if (v.duetEnabled === false) flags.push('Дуэт выкл');
  if (v.stitchEnabled === false) flags.push('Стич выкл');
  if (flags.length > 0) {
    lines.push(`${e(E.info, 'ℹ')} ${flags.join(' • ')}`);
  }

  lines.push(``);
  lines.push(`${e(E.code, '🔨')} ID: <code>${v.id}</code>`);

  // Author info
  if (v.authorNickname) {
    const avMark = v.authorVerified ? e(E.verified, '✅') : '';
    lines.push(`${e(E.profile, '👤')} Автор: <b>${escHtml(v.authorNickname)}</b> ${avMark}`);
  }

  return lines.join('\n');
}

function escHtml(str) {
  if (!str) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

// ─── Track user ───
function trackUser(msg) {
  const user = msg.from || msg;
  stmtUpsertUser.run(
    user.id,
    user.username || null,
    user.first_name || null,
    user.last_name || null
  );
}

// ─── /start ───
bot.onText(/\/start/, async (msg) => {
  trackUser(msg);
  clearState(msg.from.id);

  await bot.sendMessage(msg.chat.id, buildMainMenuText(), {
    parse_mode: 'HTML',
    reply_markup: mainMenuKeyboard(),
  });
});

// ─── /admin ───
bot.onText(/\/admin/, async (msg) => {
  if (!ADMIN_IDS.includes(msg.from.id)) {
    return bot.sendMessage(msg.chat.id, `${e(E.cross, '❌')} <b>Нет доступа</b>`, {
      parse_mode: 'HTML',
    });
  }

  trackUser(msg);

  await bot.sendMessage(msg.chat.id, `${e(E.settings, '⚙')} <b>Админ-панель</b>`, {
    parse_mode: 'HTML',
    reply_markup: adminKeyboard(),
  });
});

// ─── Callback queries ───
bot.on('callback_query', async (query) => {
  const chatId = query.message.chat.id;
  const msgId = query.message.message_id;
  const userId = query.from.id;
  const data = query.data;

  trackUser(query);

  try {
    switch (data) {
      case 'back_menu': {
        clearState(userId);
        await bot.editMessageText(buildMainMenuText(), {
          chat_id: chatId,
          message_id: msgId,
          parse_mode: 'HTML',
          reply_markup: mainMenuKeyboard(),
        });
        await bot.answerCallbackQuery(query.id);
        break;
      }

      case 'menu_profile': {
        setState(userId, 'awaiting_profile');
        await bot.editMessageText(
          [
            `${e(E.profile, '👤')} <b>Статистика профиля</b>`,
            ``,
            `${e(E.pencil, '✏')} Отправьте <b>@username</b> или ссылку на профиль TikTok:`,
          ].join('\n'),
          {
            chat_id: chatId,
            message_id: msgId,
            parse_mode: 'HTML',
            reply_markup: cancelKeyboard(),
          }
        );
        await bot.answerCallbackQuery(query.id);
        break;
      }

      case 'menu_video': {
        setState(userId, 'awaiting_video');
        await bot.editMessageText(
          [
            `${e(E.media, '🎬')} <b>Статистика видео</b>`,
            ``,
            `${e(E.link, '🔗')} Отправьте ссылку на видео TikTok:`,
          ].join('\n'),
          {
            chat_id: chatId,
            message_id: msgId,
            parse_mode: 'HTML',
            reply_markup: cancelKeyboard(),
          }
        );
        await bot.answerCallbackQuery(query.id);
        break;
      }

      case 'menu_my_stats': {
        const user = stmtGetUser.get(userId);
        const reqCount = user ? user.total_requests : 0;

        await bot.editMessageText(
          [
            `${e(E.stats, '📊')} <b>Ваша статистика</b>`,
            ``,
            `${e(E.profile, '👤')} ID: <code>${userId}</code>`,
            `${e(E.growth, '📈')} Запросов: <b>${reqCount}</b>`,
            `${e(E.calendar, '📅')} Первый визит: <b>${user?.created_at || '—'}</b>`,
            `${e(E.clock, '⏰')} Последняя активность: <b>${user?.last_active || '—'}</b>`,
          ].join('\n'),
          {
            chat_id: chatId,
            message_id: msgId,
            parse_mode: 'HTML',
            reply_markup: backToMenuKeyboard(),
          }
        );
        await bot.answerCallbackQuery(query.id);
        break;
      }

      case 'menu_info': {
        await bot.editMessageText(
          [
            `${e(E.info, 'ℹ')} <b>О боте</b>`,
            ``,
            `${e(E.bot_emoji, '🤖')} Бот для получения статистики TikTok`,
            ``,
            `${e(E.check, '✅')} <b>Возможности:</b>`,
            `• Полная статистика профиля`,
            `• Расширенная статистика видео`,
            `• Engagement Rate`,
            `• Информация о музыке и тегах`,
            ``,
            `${e(E.pencil, '✏')} Отправьте ссылку или username для начала`,
          ].join('\n'),
          {
            chat_id: chatId,
            message_id: msgId,
            parse_mode: 'HTML',
            reply_markup: backToMenuKeyboard(),
          }
        );
        await bot.answerCallbackQuery(query.id);
        break;
      }

      case 'admin_stats': {
        if (!ADMIN_IDS.includes(userId)) {
          await bot.answerCallbackQuery(query.id, { text: 'Нет доступа', show_alert: true });
          return;
        }

        const totalUsers = stmtGetStats.get().cnt;
        const totalReqs = stmtGetTotalRequests.get().cnt;

        await bot.editMessageText(
          [
            `${e(E.stats, '📊')} <b>Статистика бота</b>`,
            ``,
            `${e(E.people, '👥')} Пользователей: <b>${totalUsers}</b>`,
            `${e(E.growth, '📈')} Всего запросов: <b>${totalReqs}</b>`,
          ].join('\n'),
          {
            chat_id: chatId,
            message_id: msgId,
            parse_mode: 'HTML',
            reply_markup: adminKeyboard(),
          }
        );
        await bot.answerCallbackQuery(query.id);
        break;
      }

      default:
        await bot.answerCallbackQuery(query.id);
    }
  } catch (err) {
    console.error('Callback error:', err.message);
    try {
      await bot.answerCallbackQuery(query.id, { text: 'Произошла ошибка', show_alert: true });
    } catch (_) {}
  }
});

// ─── Text message handler ───
bot.on('message', async (msg) => {
  if (!msg.text || msg.text.startsWith('/')) return;

  const userId = msg.from.id;
  const chatId = msg.chat.id;
  const state = getState(userId);

  if (!state) return;

  trackUser(msg);

  if (state.state === 'awaiting_profile') {
    const username = extractUsername(msg.text);
    if (!username) {
      return bot.sendMessage(chatId, [
        `${e(E.cross, '❌')} <b>Неверный формат</b>`,
        ``,
        `${e(E.info, 'ℹ')} Отправьте @username или ссылку вида:`,
        `<code>https://tiktok.com/@username</code>`,
      ].join('\n'), {
        parse_mode: 'HTML',
        reply_markup: cancelKeyboard(),
      });
    }

    clearState(userId);

    const loadingMsg = await bot.sendMessage(chatId, `${e(E.loading, '🔄')} <b>Загрузка профиля @${escHtml(username)}...</b>`, {
      parse_mode: 'HTML',
    });

    const profile = await fetchTikTokProfile(username);
    stmtLogRequest.run(userId, 'profile', username, profile ? 1 : 0);

    if (!profile) {
      return bot.editMessageText(
        [
          `${e(E.cross, '❌')} <b>Профиль не найден</b>`,
          ``,
          `${e(E.info, 'ℹ')} Не удалось получить данные для @${escHtml(username)}`,
          `Проверьте правильность username или попробуйте позже.`,
        ].join('\n'),
        {
          chat_id: chatId,
          message_id: loadingMsg.message_id,
          parse_mode: 'HTML',
          reply_markup: backToMenuKeyboard(),
        }
      );
    }

    await bot.editMessageText(buildProfileText(profile), {
      chat_id: chatId,
      message_id: loadingMsg.message_id,
      parse_mode: 'HTML',
      reply_markup: profileResultKeyboard(profile.username),
    });
  }

  if (state.state === 'awaiting_video') {
    const videoInput = extractVideoId(msg.text);
    if (!videoInput) {
      return bot.sendMessage(chatId, [
        `${e(E.cross, '❌')} <b>Неверный формат</b>`,
        ``,
        `${e(E.info, 'ℹ')} Отправьте ссылку на видео:`,
        `<code>https://tiktok.com/@user/video/1234567890</code>`,
        `или <code>https://vm.tiktok.com/XXXXX/</code>`,
      ].join('\n'), {
        parse_mode: 'HTML',
        reply_markup: cancelKeyboard(),
      });
    }

    clearState(userId);

    const loadingMsg = await bot.sendMessage(chatId, `${e(E.loading, '🔄')} <b>Загрузка видео...</b>`, {
      parse_mode: 'HTML',
    });

    const video = await fetchTikTokVideo(videoInput);
    stmtLogRequest.run(userId, 'video', msg.text, video ? 1 : 0);

    if (!video) {
      return bot.editMessageText(
        [
          `${e(E.cross, '❌')} <b>Видео не найдено</b>`,
          ``,
          `${e(E.info, 'ℹ')} Не удалось получить данные.`,
          `Проверьте ссылку или попробуйте позже.`,
        ].join('\n'),
        {
          chat_id: chatId,
          message_id: loadingMsg.message_id,
          parse_mode: 'HTML',
          reply_markup: backToMenuKeyboard(),
        }
      );
    }

    await bot.editMessageText(buildVideoText(video), {
      chat_id: chatId,
      message_id: loadingMsg.message_id,
      parse_mode: 'HTML',
      reply_markup: videoResultKeyboard(video.id, video.authorUsername),
    });
  }
});

// ─── Error handling ───
bot.on('polling_error', (err) => {
  console.error('Polling error:', err.message);
});

process.on('uncaughtException', (err) => {
  console.error('Uncaught exception:', err);
});

process.on('unhandledRejection', (err) => {
  console.error('Unhandled rejection:', err);
});

console.log('🚀 TikTok Stats Bot started');
