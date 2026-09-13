// Минимальный service worker — нужен только для того, чтобы браузер разрешил
// "Установить на главный экран". Никакого офлайн-кеширования не делаем специально:
// данные приложения (мероприятия, деньги) всегда должны приходить свежими с сервера.
self.addEventListener('install', (event) => {
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(self.clients.claim());
});

self.addEventListener('fetch', (event) => {
  // просто пропускаем каждый запрос как есть, напрямую в сеть
  event.respondWith(fetch(event.request));
});

// показываем реальное уведомление, когда сервер прислал push
self.addEventListener('push', (event) => {
  let data = { title: 'Расписание студии', body: '' };
  try{ data = event.data ? event.data.json() : data; }catch(e){ /* оставляем значения по умолчанию */ }
  event.waitUntil(
    self.registration.showNotification(data.title || 'Расписание студии', {
      body: data.body || '',
      icon: '/icon-192.png',
      badge: '/icon-192.png',
    })
  );
});

// клик по уведомлению — открываем/фокусируем вкладку с приложением
self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  event.waitUntil(
    self.clients.matchAll({ type: 'window' }).then((clientsList) => {
      for (const client of clientsList) {
        if ('focus' in client) return client.focus();
      }
      if (self.clients.openWindow) return self.clients.openWindow('/');
    })
  );
});
