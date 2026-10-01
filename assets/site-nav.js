// Shared site navigation: Tools dropdown, mobile menu, current-page marker, GA clicks.
(function () {
  const nav = document.querySelector('.sbs-nav');
  if (!nav) return;
  const panel = nav.querySelector('.sbs-nav-panel');
  const menuBtn = nav.querySelector('.sbs-menu-btn');
  const toolsBtn = nav.querySelector('.sbs-tools-btn');
  const tools = nav.querySelector('.sbs-tools-menu');

  const setTools = open => { toolsBtn.setAttribute('aria-expanded', open); tools.hidden = !open; };
  toolsBtn.addEventListener('click', e => { e.stopPropagation(); setTools(tools.hidden); });
  menuBtn.addEventListener('click', () => {
    const open = !panel.classList.contains('open');
    panel.classList.toggle('open', open); menuBtn.setAttribute('aria-expanded', open);
  });
  document.addEventListener('click', e => { if (!nav.contains(e.target)) setTools(false); });
  document.addEventListener('keydown', e => { if (e.key === 'Escape') { setTools(false); panel.classList.remove('open'); } });

  // mark the current page. Street pages live under /sold-prices/<district>/<street> but
  // are street reports, so they mark "Street report"; /sold-prices and district pages
  // mark "Compare streets".
  const here = location.pathname.replace(/\.gloock\.preview$/, '').replace(/\/$/, '') || '/';
  const key = /^\/sold-prices\/[^/]+\/[^/]+$/.test(here) || here === '/street-report' ? 'report'
            : here.startsWith('/sold-prices') ? 'compare'
            : here === '/price-check' ? 'check'
            : here === '/contact' ? 'contact' : '';
  nav.querySelectorAll('.sbs-nav-panel > a.sbs-nav-link').forEach(a => {
    if (a.dataset.nav === key) a.setAttribute('aria-current', 'page');
  });
  // "Search" goes back to the homepage search; on the homepage itself it is not needed
  const search = nav.querySelector('.sbs-nav-search');
  if (search && here === '/') search.remove();

  nav.addEventListener('click', e => {
    const a = e.target.closest('a');
    if (a) { try { gtag('event', 'nav_click', { item: a.dataset.nav || '' }); } catch (_) {} }
  });
})();
