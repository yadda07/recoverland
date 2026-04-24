(function(){
  var saved = localStorage.getItem('rl-theme');
  document.documentElement.setAttribute('data-t', saved === null ? 'd' : saved);
})();

function toggleTheme(){
  var d = document.documentElement;
  var t = d.getAttribute('data-t')==='d' ? '' : 'd';
  d.setAttribute('data-t', t);
  localStorage.setItem('rl-theme', t);
}

function initScroll(){
  var links = document.querySelectorAll('.side nav a, .rp a');
  var obs = new IntersectionObserver(function(entries){
    entries.forEach(function(e){
      if(e.isIntersecting){
        links.forEach(function(a){
          a.classList.toggle('on', a.getAttribute('href')==='#'+e.target.id);
        });
      }
    });
  }, {rootMargin:'-20% 0px -70% 0px'});
  document.querySelectorAll('[id]').forEach(function(s){ obs.observe(s); });
}

function initSearch(data){
  var inp = document.getElementById('si');
  var res = document.getElementById('sr');
  if(!inp || !res) return;
  inp.addEventListener('input', function(){
    var q = inp.value.toLowerCase().trim();
    if(!q){ res.classList.remove('open'); return; }
    var hits = data.filter(function(d){
      return d.t.toLowerCase().includes(q) || d.k.includes(q);
    }).slice(0,8);
    res.innerHTML = hits.map(function(d){
      return '<div class="sr-item" onclick="location.href=\''+d.h+'\'"><span class="sr-t">'+d.t+'</span><span class="sr-k">'+d.p+'</span></div>';
    }).join('') || '<div class="sr-item"><span class="sr-t" style="color:var(--tx3)">Aucun resultat</span></div>';
    res.classList.add('open');
  });
  document.addEventListener('click', function(e){
    if(!e.target.closest('.srch')) res.classList.remove('open');
  });
}

function initProgressBar(){
  var bar = document.createElement('div'); bar.id='pgbar'; document.body.appendChild(bar);
  window.addEventListener('scroll', function(){
    var d = document.documentElement;
    bar.style.width = (d.scrollHeight > d.clientHeight ? d.scrollTop/(d.scrollHeight-d.clientHeight)*100 : 0) + '%';
  }, {passive:true});
}

document.addEventListener('keydown', function(e){
  if(e.key==='/' && document.activeElement.tagName!=='INPUT'){
    e.preventDefault(); var inp=document.getElementById('si'); if(inp){inp.focus();inp.select();}
  }
});

var RL_SEARCH = [
  {t:'Introduction',k:'recoverland plugin qgis audit',h:'index.html#intro',p:'index'},
  {t:'Architecture',k:'architecture sqlite journal tracker',h:'index.html#architecture',p:'index'},
  {t:'Frontiere locale',k:'local trust boundary local only workstation sqlite no service',h:'index.html#trust-boundary',p:'index'},
  {t:'Modele de donnees local',k:'mcd data model sqlite audit_session datasource_registry backend_settings schema_version',h:'index.html#data-model',p:'index'},
  {t:'Journal SQLite',k:'sqlite journal wal audit_event',h:'index.html#journal',p:'index'},
  {t:'Capture automatique',k:'capture tracker edit commit signal',h:'index.html#capture',p:'index'},
  {t:'Tampon d\'edition',k:'edit buffer session snapshot net effect',h:'index.html#edit-buffer',p:'index'},
  {t:'Stockage delta',k:'delta update changed_only attributes storage',h:'index.html#delta-storage',p:'index'},
  {t:'Identification',k:'fingerprint datasource identity pk fid',h:'index.html#identification',p:'index'},
  {t:'Restauration intelligente',k:'smart restore entity fingerprint restored_from_event_id suppress trace dedup',h:'index.html#restore-dedup',p:'index'},
  {t:'Force d\'identification',k:'strong medium weak none identity strength',h:'index.html#identity-strength',p:'index'},
  {t:'Derive de schema',k:'schema drift migration champ ajoute supprime',h:'index.html#schema-drift',p:'index'},
  {t:'Integrite et recuperation',k:'integrity wal checkpoint pending recovery crash',h:'index.html#integrity',p:'index'},
  {t:'Retention et purge',k:'retention purge vacuum age session compactage',h:'index.html#retention',p:'index'},
  {t:'Cycle de vie',k:'event lifecycle writequeue commit rollback',h:'index.html#event-lifecycle',p:'index'},
  {t:'Installation',k:'installer qgis plugin manager',h:'guide.html#installation',p:'guide'},
  {t:'Premier lancement',k:'premier lancement projet ouvrir',h:'guide.html#first-launch',p:'guide'},
  {t:'Rechercher des modifications',k:'recherche recover filtre date',h:'guide.html#search',p:'guide'},
  {t:'Restaurer des donnees',k:'restaurer restore selection',h:'guide.html#restore',p:'guide'},
  {t:'GeoPackage',k:'gpkg geopackage strong identity',h:'formats.html#geopackage',p:'formats'},
  {t:'Shapefile',k:'shp shapefile medium fid instable',h:'formats.html#shapefile',p:'formats'},
  {t:'PostgreSQL / PostGIS',k:'postgres postgis pk strong',h:'formats.html#postgresql',p:'formats'},
  {t:'CSV / Texte',k:'csv delimited text weak',h:'formats.html#csv',p:'formats'},
  {t:'Fichier deplace',k:'deplace rename chemin fingerprint orphelin',h:'limits.html#file-moved',p:'limites'},
  {t:'Lecteur reseau',k:'reseau unc montage lecteur',h:'limits.html#network-drive',p:'limites'},
  {t:'FID instable',k:'fid shapefile compactage repack',h:'limits.html#fid-instable',p:'limites'},
  {t:'Couche memoire',k:'memory volatile temporaire',h:'limits.html#memory-layer',p:'limites'},
];

function initReveal(){
  var selectors = [
    {sel:'section',cls:'reveal'},
    {sel:'.cards',cls:'stagger'},
    {sel:'.hero-badges',cls:'stagger'},
    {sel:'.info,.warn,.tip,.danger',cls:''},
    {sel:'.tw',cls:''},
    {sel:'.dia',cls:''},
    {sel:'.flow',cls:''},
    {sel:'.step',cls:'reveal'},
    {sel:'pre',cls:''},
  ];
  var targets = [];
  selectors.forEach(function(s){
    document.querySelectorAll('.main '+s.sel).forEach(function(el){
      if(s.cls && !el.classList.contains(s.cls)) el.classList.add(s.cls);
      targets.push(el);
    });
  });
  var hero = document.querySelector('.hero');
  if(hero){
    var logo = hero.querySelector('.hero-logo');
    var sub = hero.querySelector('.hero-sub');
    var line = hero.querySelector('.hero-line');
    var badges = hero.querySelector('.hero-badges');
    [logo,sub,line,badges].forEach(function(el){ if(el) targets.push(el); });
    if(badges && !badges.classList.contains('stagger')) badges.classList.add('stagger');
  }
  var obs = new IntersectionObserver(function(entries){
    entries.forEach(function(e){
      if(e.isIntersecting){
        e.target.classList.add('vis');
        obs.unobserve(e.target);
      }
    });
  },{threshold:0.12,rootMargin:'0px 0px -40px 0px'});
  targets.forEach(function(t){ obs.observe(t); });
}

function initSmoothNav(){
  document.querySelectorAll('.side nav a, .rp a, .bar-nav a[href^="#"]').forEach(function(a){
    a.addEventListener('click',function(e){
      var href = a.getAttribute('href');
      if(href && href.charAt(0)==='#'){
        var target = document.querySelector(href);
        if(target){
          e.preventDefault();
          target.scrollIntoView({behavior:'smooth',block:'start'});
          history.pushState(null,null,href);
        }
      }
    });
  });
}

/* ===== HERO GRADIENT ORBS ===== */
function initHeroOrbs(){
  var hero = document.querySelector('.hero');
  if(!hero) return;
  var orb1 = document.createElement('div');
  orb1.className = 'hero-orb';
  var orb2 = document.createElement('div');
  orb2.className = 'hero-orb-2';
  hero.insertBefore(orb2, hero.firstChild);
  hero.insertBefore(orb1, hero.firstChild);
}

/* ===== HEADING TEXT SPLIT REVEAL ===== */
function initHeadingReveal(){
  var prefersReduced = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  if(prefersReduced) return;
  document.querySelectorAll('.main h2').forEach(function(h2){
    var text = h2.textContent;
    if(!text.trim()) return;
    var span = document.createElement('span');
    span.className = 'h-reveal';
    var chars = text.split('');
    chars.forEach(function(c, i){
      var ch = document.createElement('span');
      ch.className = 'h-char';
      ch.textContent = c === ' ' ? '\u00A0' : c;
      ch.style.transitionDelay = (i * 0.02) + 's';
      span.appendChild(ch);
    });
    h2.textContent = '';
    h2.appendChild(span);
    var obs = new IntersectionObserver(function(entries){
      entries.forEach(function(e){
        if(e.isIntersecting){
          span.classList.add('vis');
          obs.unobserve(e.target);
        }
      });
    },{threshold:0.3});
    obs.observe(h2);
  });
}

/* ===== SIDEBAR MORPHING PILL ===== */
function initSidebarPill(){
  var nav = document.querySelector('.side nav');
  if(!nav) return;
  var pill = document.createElement('div');
  pill.className = 'side-pill';
  nav.appendChild(pill);
  function movePill(){
    var active = nav.querySelector('a.on');
    if(!active){
      pill.classList.remove('active');
      return;
    }
    var navRect = nav.getBoundingClientRect();
    var aRect = active.getBoundingClientRect();
    pill.style.top = (aRect.top - navRect.top + nav.scrollTop) + 'px';
    pill.style.height = aRect.height + 'px';
    pill.style.width = aRect.width + 'px';
    pill.classList.add('active');
  }
  var mo = new MutationObserver(function(muts){
    muts.forEach(function(m){
      if(m.type === 'attributes' && m.attributeName === 'class') movePill();
    });
  });
  nav.querySelectorAll('a').forEach(function(a){
    mo.observe(a, {attributes:true, attributeFilter:['class']});
  });
  movePill();
  window.addEventListener('resize', movePill, {passive:true});
}

/* ===== HERO SCROLL PARALLAX ===== */
function initHeroParallax(){
  var hero = document.querySelector('.hero');
  if(!hero) return;
  var logo = hero.querySelector('.hero-logo');
  var sub = hero.querySelector('.hero-sub');
  var badges = hero.querySelector('.hero-badges');
  var line = hero.querySelector('.hero-line');
  if(window.matchMedia('(prefers-reduced-motion: reduce)').matches) return;
  var ticking = false;
  window.addEventListener('scroll', function(){
    if(!ticking){
      ticking = true;
      requestAnimationFrame(function(){
        var rect = hero.getBoundingClientRect();
        var h = hero.offsetHeight;
        if(rect.bottom < 0 || rect.top > window.innerHeight){ ticking = false; return; }
        var progress = Math.max(0, Math.min(1, -rect.top / h));
        var opacity = 1 - progress * 1.2;
        var scale = 1 - progress * 0.08;
        var ty = progress * 30;
        if(logo && logo.classList.contains('vis')){
          logo.style.transform = 'translateY(' + ty + 'px) scale(' + scale + ')';
          logo.style.opacity = Math.max(0, opacity);
        }
        if(sub) sub.style.opacity = Math.max(0, 1 - progress * 1.8);
        if(badges) badges.style.opacity = Math.max(0, 1 - progress * 2);
        if(line) line.style.opacity = Math.max(0, (0.5 - progress * 1.5));
        ticking = false;
      });
    }
  }, {passive:true});
}

/* ===== VIEW TRANSITIONS NAVIGATION ===== */
function initViewTransitions(){
  if(!document.startViewTransition) return;
  document.querySelectorAll('.bar-nav a[href], .side nav a[href]').forEach(function(a){
    var href = a.getAttribute('href');
    if(!href || href.charAt(0) === '#') return;
    a.addEventListener('click', function(e){
      e.preventDefault();
      document.startViewTransition(function(){
        window.location.href = href;
      });
    });
  });
}

/* ===== CALLOUT MOUSE GLOW ===== */
function initCalloutGlow(){
  var prefersReduced = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  if(prefersReduced) return;
  document.querySelectorAll('.info,.warn,.tip,.danger').forEach(function(el){
    var glow = document.createElement('div');
    glow.style.cssText = 'position:absolute;width:180px;height:180px;border-radius:50%;pointer-events:none;z-index:0;opacity:0;transition:opacity .3s ease;';
    glow.className = 'callout-glow-orb';
    var c1 = getComputedStyle(el).getPropertyValue('--c1').trim() || '#2563EB';
    glow.style.background = 'radial-gradient(circle,color-mix(in srgb,' + c1 + ' 12%,transparent) 0%,transparent 70%)';
    el.appendChild(glow);
    el.addEventListener('mousemove', function(e){
      var rect = el.getBoundingClientRect();
      var x = e.clientX - rect.left - 90;
      var y = e.clientY - rect.top - 90;
      glow.style.transform = 'translate(' + x + 'px,' + y + 'px)';
      glow.style.opacity = '1';
    });
    el.addEventListener('mouseleave', function(){
      glow.style.opacity = '0';
    });
  });
}

/* ===== CARD 3D TILT ===== */
function initCardTilt(){
  var prefersReduced = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  if(prefersReduced) return;
  document.querySelectorAll('.card').forEach(function(card){
    card.addEventListener('mousemove', function(e){
      var rect = card.getBoundingClientRect();
      var x = e.clientX - rect.left;
      var y = e.clientY - rect.top;
      var cx = rect.width / 2;
      var cy = rect.height / 2;
      var rotateX = ((y - cy) / cy) * -4;
      var rotateY = ((x - cx) / cx) * 4;
      card.style.transform = 'perspective(800px) rotateX('+rotateX+'deg) rotateY('+rotateY+'deg) translateY(-4px)';
      card.classList.add('tilt-active');
    });
    card.addEventListener('mouseleave', function(){
      card.style.transform = '';
      card.classList.remove('tilt-active');
    });
  });
}

/* ===== SVG LINE DRAWING ===== */
function initSvgDraw(){
  document.querySelectorAll('.dia svg line, .dia svg .edge, .dia svg .edge-ac').forEach(function(el){
    if(el.tagName === 'line' || el.tagName === 'LINE'){
      var x1 = parseFloat(el.getAttribute('x1') || 0);
      var y1 = parseFloat(el.getAttribute('y1') || 0);
      var x2 = parseFloat(el.getAttribute('x2') || 0);
      var y2 = parseFloat(el.getAttribute('y2') || 0);
      var len = Math.sqrt(Math.pow(x2-x1,2) + Math.pow(y2-y1,2));
      el.classList.add('draw');
      el.style.setProperty('--len', Math.ceil(len));
    }
  });
}

/* ===== AUTO-GENERATE RIGHT SIDEBAR TOC ===== */
function initRightToc(){
  var container = document.getElementById('rp-toc');
  if(!container) return;
  var headings = document.querySelectorAll('.main h2, .main h3');
  headings.forEach(function(h){
    var id = h.id;
    if(!id && h.tagName === 'H2'){
      var sec = h.closest('section[id]');
      if(sec) id = sec.id;
    }
    if(!id){
      var slug = h.textContent.trim().toLowerCase()
        .replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
      var base = slug; var i = 1;
      while(document.getElementById(slug)){ slug = base + '-' + i; i++; }
      h.id = slug;
      id = slug;
    }
    if(!id) return;
    var a = document.createElement('a');
    a.href = '#' + id;
    a.textContent = h.textContent;
    if(h.tagName === 'H3') a.className = 'sub';
    container.appendChild(a);
  });
}

/* ===== SMOOTH SCROLL PROGRESS FOR TOC ===== */
function initTocProgress(){
  var rpLinks = document.querySelectorAll('.rp a[href^="#"]');
  if(!rpLinks.length) return;
  var sections = [];
  rpLinks.forEach(function(a){
    var id = a.getAttribute('href').slice(1);
    var el = document.getElementById(id);
    if(el) sections.push({el:el, link:a});
  });
  var lastActive = null;
  window.addEventListener('scroll', function(){
    var scrollY = window.scrollY + window.innerHeight * 0.3;
    var active = null;
    sections.forEach(function(s){
      if(s.el.offsetTop <= scrollY) active = s;
    });
    if(active && active !== lastActive){
      if(lastActive) lastActive.link.classList.remove('on');
      active.link.classList.add('on');
      lastActive = active;
    }
  }, {passive:true});
}

document.addEventListener('DOMContentLoaded', function(){
  initRightToc();
  initScroll();
  initProgressBar();
  initSearch(RL_SEARCH);
  initHeroOrbs();
  initReveal();
  initHeadingReveal();
  initCalloutGlow();
  initCardTilt();
  initSvgDraw();
  initSmoothNav();
  initTocProgress();
  initSidebarPill();
  initHeroParallax();
  initViewTransitions();
});
