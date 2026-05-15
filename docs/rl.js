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
  {t:'Architecture vision',k:'architecture vision big picture overview',h:'architecture.html#vision',p:'architecture'},
  {t:'Six layers',k:'layers entry ui threads orchestration core infra',h:'architecture.html#layers',p:'architecture'},
  {t:'Module graph',k:'dependency graph imports modules core',h:'architecture.html#deps',p:'architecture'},
  {t:'Threading model',k:'threads ui writer search stats wal',h:'architecture.html#threads',p:'architecture'},
  {t:'Capture pipeline',k:'capture pipeline signal tracker buffer write queue sqlite',h:'architecture.html#capture-pipe',p:'architecture'},
  {t:'Restore pipeline',k:'restore pipeline plan preflight executor service trace',h:'architecture.html#restore-pipe',p:'architecture'},
  {t:'Entry and lifecycle',k:'recover init plugin factory bootstrap signals',h:'architecture.html#cat-entry',p:'architecture'},
  {t:'UI modules',k:'dialog widgets info bar maintenance status bar',h:'architecture.html#cat-ui',p:'architecture'},
  {t:'Background threads catalog',k:'local search stats version fetch task support',h:'architecture.html#cat-threads',p:'architecture'},
  {t:'Contracts and types',k:'audit backend restore contracts support policy audit field policy',h:'architecture.html#cat-contracts',p:'architecture'},
  {t:'Identity and data modules',k:'identity serialization geometry schema drift user identity',h:'architecture.html#cat-identity',p:'architecture'},
  {t:'Capture path modules',k:'edit tracker edit buffer write queue capture',h:'architecture.html#cat-capture',p:'architecture'},
  {t:'Storage and registry',k:'sqlite schema journal manager backend datasource registry alias settings',h:'architecture.html#cat-storage',p:'architecture'},
  {t:'Read and search',k:'search service event stream repository journal audit stats cache',h:'architecture.html#cat-read',p:'architecture'},
  {t:'Restore engine modules',k:'rewind dedup restore planner executor service workflow preview geometry preview',h:'architecture.html#cat-restore',p:'architecture'},
  {t:'Health and maintenance',k:'health monitor disk monitor integrity retention db maintenance vacuum purge',h:'architecture.html#cat-health',p:'architecture'},
  {t:'Infrastructure modules',k:'compat logger sql safety observability time format constants',h:'architecture.html#cat-infra',p:'architecture'},
  {t:'Tech debt map',k:'debt monolith dialog restore matching duplication exports',h:'architecture.html#debt',p:'architecture'},
  {t:'Architecture (legacy)',k:'architecture sqlite journal tracker',h:'index.html#architecture',p:'index'},
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

/* ===================================================================
   INTERACTIVE ARCHITECTURE GRAPH
   Yellow + grayscale only. Hover = highlight, others fade.
   Typewriter description streams in. Click chips to jump between modules.
   =================================================================== */

var ARCH_DATA = {
  clusters: [
    {id:'health',   label:'Health & Maintenance', x: 30,  y: 30,  w: 360,  h: 280},
    {id:'ui',       label:'UI Surface',           x: 410, y: 30,  w: 660,  h: 280},
    {id:'threads',  label:'Background Threads',   x: 1090,y: 30,  w: 380,  h: 280},
    {id:'identity', label:'Identity & Data',      x: 30,  y: 330, w: 360,  h: 290},
    {id:'core',     label:'Entry / Contracts',    x: 410, y: 330, w: 660,  h: 290},
    {id:'capture',  label:'Capture Pipeline',     x: 1090,y: 330, w: 380,  h: 290},
    {id:'restore',  label:'Restore Engine',       x: 30,  y: 640, w: 540,  h: 280},
    {id:'read',     label:'Read & Search',        x: 590, y: 640, w: 320,  h: 280},
    {id:'storage',  label:'Storage & Registry',   x: 930, y: 640, w: 540,  h: 280},
    {id:'infra',    label:'Infrastructure (transverse)', x: 30, y: 935, w: 1440, h: 50}
  ],
  nodes: [
    /* HEALTH */
    {id:'integrity', label:'integrity', x:110, y:100, r:20, cluster:'health', lines:261,
     desc:'Startup integrity check. PRAGMA integrity_check, WAL checkpoint, schema version verification. Reads recoverland_pending.json (events that did not reach SQLite on the previous run) and replays them.',
     deps:['sqlite_schema','write_queue']},
    {id:'retention', label:'retention', x:110, y:185, r:18, cluster:'health', lines:187,
     desc:'Purge by age and by volume. 5k-row batches. Async VACUUM under a mutex. Defaults: 365 days, 1M events maximum.',
     deps:['audit_backend','sqlite_schema']},
    {id:'db_maintenance', label:'db_maintenance', x:110, y:265, r:18, cluster:'health', lines:71,
     desc:'Periodic ANALYZE, quick integrity check, grouped WAL checkpoint. Safe with concurrent readers.',
     deps:[]},
    {id:'health_monitor', label:'health_monitor', x:260, y:115, r:22, cluster:'health', lines:206,
     desc:'Evaluates HEALTHY / INFO / WARNING / CRITICAL based on size, event count, age. Produces translated user messages and remediation suggestions.',
     deps:['audit_backend']},
    {id:'disk_monitor', label:'disk_monitor', x:330, y:230, r:18, cluster:'health', lines:68,
     desc:'Free-disk check on the journal volume. Triggers tracking disable below the critical threshold (100 MB).',
     deps:[]},

    /* UI */
    {id:'widgets', label:'widgets/', x:530, y:80, r:22, cluster:'ui', lines:585,
     desc:'Five widgets: time_slider (cutoff date), restore_mode_selector (A/B switch), restore_preflight_dialog, toggle_switch, themed_logo.',
     deps:[]},
    {id:'status_bar_widget', label:'status_bar_widget', x:730, y:80, r:20, cluster:'ui', lines:93,
     desc:'Persistent indicator in the QGIS status bar. Left-click toggles tracking, right-click opens the dialog.',
     deps:[]},
    {id:'themed_action_icon', label:'themed_action_icon', x:920, y:80, r:14, cluster:'ui', lines:123,
     desc:'SVG toolbar icon recoloured to match QGIS light/dark theme at runtime.',
     deps:['compat']},
    {id:'journal_info_bar', label:'journal_info_bar', x:560, y:215, r:20, cluster:'ui', lines:236,
     desc:'Smart bar at the top of the dialog: per-operation tile counters, color-coded health pill.',
     deps:['journal_audit','health_monitor']},
    {id:'recover_dialog', label:'recover_dialog', x:760, y:225, r:30, cluster:'ui', lines:2797,
     desc:'Main dialog. 2797 lines, the largest file. Mixes widget construction, restore orchestration, geometry preview, smart bar, state machine. Largest tech debt.',
     deps:['restore_runner','search_service','geometry_preview','journal_info_bar','journal_maintenance','widgets','time_format']},
    {id:'journal_maintenance', label:'journal_maintenance', x:980, y:215, r:20, cluster:'ui', lines:309,
     desc:'Maintenance dialog. Retention config, manual purge, async VACUUM, integrity check, journal export.',
     deps:['retention','integrity','db_maintenance','time_format']},

    /* THREADS */
    {id:'version_fetch_thread', label:'version_fetch_thread', x:1200, y:80, r:18, cluster:'threads', lines:104,
     desc:'Worker thread that fetches post-cutoff events for the Rewind preview.',
     deps:['event_stream_repository']},
    {id:'local_search_thread', label:'local_search_thread', x:1380, y:80, r:18, cluster:'threads', lines:74,
     desc:'Worker thread running paginated search. Emits result via Qt signal.',
     deps:['search_service']},
    {id:'journal_stats_thread', label:'journal_stats_thread', x:1200, y:215, r:18, cluster:'threads', lines:118,
     desc:'Debounced (300 ms) aggregate query for the smart bar.',
     deps:['journal_audit','layer_stats_cache']},
    {id:'qgs_task_support', label:'qgs_task_support', x:1380, y:215, r:16, cluster:'threads', lines:64,
     desc:'Abstraction over QThread / QgsTask. Same API on QGIS 3.40 (Qt5) and 4.x (Qt6).',
     deps:['compat']},

    /* IDENTITY */
    {id:'support_policy', label:'support_policy', x:110, y:380, r:20, cluster:'identity', lines:136,
     desc:'Per-provider capture/restore matrix. Decides if a layer is FULL/PARTIAL/INFO support and STRONG/MEDIUM/WEAK identity.',
     deps:['compat']},
    {id:'audit_field_policy', label:'audit_field_policy', x:250, y:380, r:18, cluster:'identity', lines:51,
     desc:'Single source of truth for audit metadata field names (date_modif, updated_at, gid, ...). Used by capture, delta and restore so they all agree on what to ignore.',
     deps:[]},
    {id:'user_identity', label:'user_identity', x:350, y:380, r:14, cluster:'identity', lines:68,
     desc:'Resolves the current user name. plugin config -> RECOVERLAND_USER env -> OS login -> QGIS profile -> unknown.',
     deps:[]},
    {id:'identity', label:'identity', x:130, y:475, r:22, cluster:'identity', lines:165,
     desc:'Datasource and feature fingerprints. Normalizes PostgreSQL / MSSQL / Oracle / OGR URIs into canonical strings. SHA-256 hashing of identity payloads.',
     deps:['compat']},
    {id:'geometry_utils', label:'geometry_utils', x:280, y:475, r:20, cluster:'identity', lines:188,
     desc:'WKB and QgsGeometry conversion. Comparison, feature matching, CRS extraction, provider geometry probing.',
     deps:['compat']},
    {id:'serialization', label:'serialization', x:130, y:570, r:20, cluster:'identity', lines:189,
     desc:'QVariant to JSON-safe values. compute_update_delta() for old/new attribute diff. iter_mapped_attributes() applies field mapping at restore.',
     deps:['compat']},
    {id:'schema_drift', label:'schema_drift', x:280, y:570, r:18, cluster:'identity', lines:142,
     desc:'Compares field schema captured at audit time with current layer. Produces matched / missing / added / type-changed report.',
     deps:['compat']},

    /* ENTRY + CONTRACTS (cluster: core) */
    {id:'__init__', label:'__init__', x:760, y:380, r:16, cluster:'core', lines:30,
     desc:'QGIS plugin factory. Compiles translations if needed. Returns RecoverPlugin instance.',
     deps:['recover']},
    {id:'audit_backend', label:'audit_backend', x:600, y:475, r:22, cluster:'core', lines:76,
     desc:'Defines AuditEvent (21 fields), SearchCriteria, SearchResult, RestoreReport, and the abstract AuditBackend interface. Pure data shapes, zero QGIS, zero Qt.',
     deps:[]},
    {id:'recover', label:'recover', x:760, y:475, r:32, cluster:'core', lines:538,
     desc:'Plugin orchestrator. Detects duplicate installs, opens journal, starts the writer queue, instantiates edit tracker, wires QGIS project signals (layersAdded, cleared, readProject), schedules orphan cleanup, periodic disk-space check.',
     deps:['edit_tracker','write_queue','recover_dialog','journal_manager','sqlite_backend','integrity','disk_monitor','status_bar_widget']},
    {id:'restore_contracts', label:'restore_contracts', x:920, y:475, r:22, cluster:'core', lines:164,
     desc:'Enums (RestoreMode, ConflictPolicy, AtomicityPolicy, PreflightVerdict), PlannedAction, RestorePlan, PreflightReport, COMPENSATORY_OPS matrix.',
     deps:[]},

    /* CAPTURE */
    {id:'edit_tracker', label:'edit_tracker', x:1180, y:400, r:26, cluster:'capture', lines:802,
     desc:'Core of capture. Connects to six QGIS signals per layer, snapshots before commit, builds AuditEvents after commit, hands them to write_queue.',
     deps:['edit_buffer','write_queue','identity','serialization','geometry_utils','audit_field_policy','support_policy','audit_backend']},
    {id:'edit_buffer', label:'edit_buffer', x:1340, y:400, r:20, cluster:'capture', lines:213,
     desc:'In-memory feature snapshots per session per layer. Bounded at 10000 features / 200 MB. Only the first snapshot per feature is kept (the pre-edit state).',
     deps:[]},
    {id:'write_queue', label:'write_queue', x:1240, y:540, r:22, cluster:'capture', lines:244,
     desc:'Dedicated writer thread (RecoverLand-Writer). Bounded queue 50k. Batch executemany of 500 rows. 3-retry policy. Passive WAL checkpoint every 60 s. Pending JSON sidecar on overflow.',
     deps:['sqlite_schema','integrity','audit_backend']},

    /* RESTORE */
    {id:'workflow_service', label:'workflow_service', x:90, y:680, r:20, cluster:'restore', lines:181,
     desc:'Groups events by datasource fingerprint, finds target layer, orchestrates per-group restore and per-group undo. Cleans up temporary layers added during restore.',
     deps:['restore_executor','restore_service','datasource_registry']},
    {id:'restore_runner', label:'restore_runner', x:240, y:680, r:22, cluster:'restore', lines:280,
     desc:'UI-thread chunked driver (QTimer). Three runners: event-based, strict rewind, undo. Emits progress signal.',
     deps:['restore_planner','restore_executor','workflow_service']},
    {id:'restore_planner', label:'restore_planner', x:390, y:680, r:22, cluster:'restore', lines:203,
     desc:'Builds the RestorePlan. Mode A iterates selected events. Mode B calls stream repository + dedup. Runs preflight (volume / drift / coverage). Pure data output, zero QGIS.',
     deps:['rewind_dedup','event_stream_repository','restore_contracts','schema_drift']},
    {id:'restore_preview', label:'restore_preview', x:510, y:690, r:14, cluster:'restore', lines:77,
     desc:'Formats RestorePlan and PreflightReport into a human-readable summary for the confirmation dialog.',
     deps:['restore_contracts']},
    {id:'restore_service', label:'restore_service', x:140, y:790, r:24, cluster:'restore', lines:823,
     desc:'Feature-by-feature primitives. Re-insert deleted, revert updated, delete inserted. Hosts _find_by_snapshot with six fallback levels. Where 90% of historical bugs lived.',
     deps:['identity','geometry_utils','serialization','audit_field_policy','support_policy']},
    {id:'restore_executor', label:'restore_executor', x:290, y:800, r:24, cluster:'restore', lines:624,
     desc:'Applies plan on QGIS layer. Two strategies. STRICT (editing buffer + rollback) and BEST_EFFORT (per-entity direct). Checks provider capabilities before acting.',
     deps:['restore_service','restore_contracts','geometry_utils']},
    {id:'rewind_dedup', label:'rewind_dedup', x:430, y:790, r:20, cluster:'restore', lines:229,
     desc:'Receives N events post-cutoff. Filters trace events and invalidated ones. Eliminates user events already compensated by traces. Pure deterministic logic, zero QGIS.',
     deps:['restore_contracts','audit_backend']},
    {id:'geometry_preview', label:'geometry_preview', x:90, y:880, r:14, cluster:'restore', lines:76,
     desc:'Displays captured geometry on QGIS canvas as QgsRubberBand. One preview at a time, cleaned on dialog close.',
     deps:['geometry_utils']},

    /* READ */
    {id:'search_service', label:'search_service', x:660, y:680, r:22, cluster:'read', lines:272,
     desc:'Paginated search with multi-criteria filtering. Lightweight mode strips geometry BLOBs. count_events, get_event_by_id, get_distinct_layers, summarize_scope.',
     deps:['audit_backend','datasource_alias','identity']},
    {id:'event_stream_repository', label:'event_stream_repo', x:830, y:680, r:20, cluster:'read', lines:161,
     desc:'Temporal queries for restore. Entity stream, events after cutoff (DESC for reverse replay). All bounded by MAX_EVENTS_PER_RESTORE.',
     deps:['audit_backend']},
    {id:'journal_audit', label:'journal_audit', x:660, y:820, r:18, cluster:'read', lines:143,
     desc:'Single-query introspection. Top N users, top N layers, per-operation counts, time range. Zero QGIS, safe for workers.',
     deps:['audit_backend']},
    {id:'layer_stats_cache', label:'layer_stats_cache', x:830, y:820, r:16, cluster:'read', lines:95,
     desc:'Cache of min/max dates and operation types per datasource. Built in one GROUP BY. Thread-safe for reads after build.',
     deps:[]},

    /* STORAGE */
    {id:'sqlite_backend', label:'sqlite_backend', x:980, y:680, r:18, cluster:'storage', lines:54,
     desc:'Facade implementing AuditBackend. Delegates writes to write_queue, reads to search_service.',
     deps:['write_queue','search_service','sqlite_schema','audit_backend']},
    {id:'journal_manager', label:'journal_manager', x:1140, y:680, r:22, cluster:'storage', lines:324,
     desc:'Locates or creates the SQLite file. Saved project: .recoverland/ next to .qgz. Unsaved: QGIS profile under content hash. PID-based file lock against duplicate QGIS instances.',
     deps:['sqlite_schema']},
    {id:'sqlite_schema', label:'sqlite_schema', x:1330, y:680, r:22, cluster:'storage', lines:269,
     desc:'DDL of six tables, ten indexes. PRAGMAs (WAL, mmap, cache, busy_timeout). Migration ladder v1 to v5. Schema version table.',
     deps:[]},
    {id:'local_settings', label:'local_settings', x:980, y:820, r:16, cluster:'storage', lines:87,
     desc:'Per-project settings persisted in backend_settings: retention days, max events, capture toggle, user override.',
     deps:[]},
    {id:'datasource_alias', label:'datasource_alias', x:1140, y:820, r:18, cluster:'storage', lines:125,
     desc:'Links an old fingerprint to a new one when a layer moves. Transitive resolution bounded to 8 hops to prevent cycles.',
     deps:['identity']},
    {id:'datasource_registry', label:'datasource_registry', x:1330, y:820, r:20, cluster:'storage', lines:223,
     desc:'Stores URI / provider / authcfg / CRS / geometry type at first commit. Used at restore time to recreate a layer. Resolves DB credentials via QGIS saved connections (passwords never persisted).',
     deps:['identity']},

    /* INFRA */
    {id:'compat', label:'compat', x:230, y:960, r:22, cluster:'infra', lines:254,
     desc:'Single source for Qt5/Qt6 and QGIS 3.40/4.x divergence. All Qt.X, Qgis.X, QgsWkbTypes.Y, QgsVectorDataProvider.Capability.Z go through here. Direct access elsewhere is forbidden.',
     deps:[]},
    {id:'logger', label:'logger', x:540, y:960, r:20, cluster:'infra', lines:116,
     desc:'Rotating file logger (5x5 MB) in QGIS profile + QgsMessageLog mirror. flog(), qlog(), timed_op() context manager, generate_trace_id() for correlation.',
     deps:[]},
    {id:'time_format', label:'time_format', x:820, y:960, r:14, cluster:'infra', lines:119,
     desc:'Human-friendly time formatting (UX-E01). Converts ISO timestamps into relative ("a l\'instant", "il y a 5 min") or short absolute strings. Used by the smart bar and the maintenance dialog.',
     deps:[]},
    {id:'sql_safety', label:'sql_safety', x:1000, y:960, r:14, cluster:'infra', lines:28,
     desc:'Defense-in-depth assertion. Any f-string SQL fragment passes through assert_safe_fragment(). Values are always parameterised separately.',
     deps:[]},
    {id:'observability', label:'observability', x:1280, y:960, r:20, cluster:'infra', lines:262,
     desc:'CycleStats accumulator. log_cycle_summary() emits one summary line plus anomaly lines. log_state_transition() and assert_invariant() escalate to CRITICAL on violation.',
     deps:['logger']}
  ]
};

/* Verb dictionary : describes the actual semantic of each dependency arrow.
   Key format : "from|to". Read as a sentence : "<from> <verb> <to>".
   Missing entries fall back to a generic "uses" in the renderer.            */
var ARCH_VERBS = {
  /* health */
  'integrity|sqlite_schema':'runs PRAGMA on',
  'integrity|write_queue':'replays pending into',
  'retention|audit_backend':'reads event types from',
  'retention|sqlite_schema':'batch deletes via',
  'health_monitor|audit_backend':'introspects',
  /* ui */
  'themed_action_icon|compat':'Qt5/6 via',
  'journal_info_bar|journal_audit':'queries',
  'journal_info_bar|health_monitor':'reads status from',
  'recover_dialog|restore_runner':'drives restore via',
  'recover_dialog|search_service':'queries',
  'recover_dialog|geometry_preview':'previews via',
  'recover_dialog|journal_info_bar':'embeds',
  'recover_dialog|journal_maintenance':'opens',
  'recover_dialog|widgets':'composes from',
  'recover_dialog|time_format':'formats dates via',
  'journal_maintenance|retention':'configures',
  'journal_maintenance|integrity':'runs',
  'journal_maintenance|db_maintenance':'schedules',
  'journal_maintenance|time_format':'formats dates via',
  /* threads */
  'version_fetch_thread|event_stream_repository':'fetches via',
  'local_search_thread|search_service':'runs',
  'journal_stats_thread|journal_audit':'aggregates via',
  'journal_stats_thread|layer_stats_cache':'warms',
  'qgs_task_support|compat':'abstracts Qt via',
  /* identity */
  'support_policy|compat':'Qgis enums via',
  'identity|compat':'QVariant via',
  'geometry_utils|compat':'QgsGeometry via',
  'serialization|compat':'QVariant via',
  'schema_drift|compat':'field types via',
  /* core */
  '__init__|recover':'instantiates',
  'recover|edit_tracker':'starts capture via',
  'recover|write_queue':'launches writer',
  'recover|recover_dialog':'opens',
  'recover|journal_manager':'locates db with',
  'recover|sqlite_backend':'uses as backend',
  'recover|integrity':'checks at startup',
  'recover|disk_monitor':'schedules',
  'recover|status_bar_widget':'wires in QGIS',
  /* capture */
  'edit_tracker|edit_buffer':'snapshots into',
  'edit_tracker|write_queue':'submits events to',
  'edit_tracker|identity':'fingerprints via',
  'edit_tracker|serialization':'encodes via',
  'edit_tracker|geometry_utils':'captures geom with',
  'edit_tracker|audit_field_policy':'ignores fields per',
  'edit_tracker|support_policy':'checks layer via',
  'edit_tracker|audit_backend':'produces events of',
  'write_queue|sqlite_schema':'writes to',
  'write_queue|integrity':'spills pending to',
  'write_queue|audit_backend':'serializes',
  /* restore */
  'workflow_service|restore_executor':'delegates apply to',
  'workflow_service|restore_service':'uses primitives of',
  'workflow_service|datasource_registry':'finds layer via',
  'restore_runner|restore_planner':'asks plan of',
  'restore_runner|restore_executor':'drives apply on',
  'restore_runner|workflow_service':'delegates multi-layer to',
  'restore_planner|rewind_dedup':'filters via',
  'restore_planner|event_stream_repository':'fetches stream via',
  'restore_planner|restore_contracts':'outputs',
  'restore_planner|schema_drift':'preflight via',
  'restore_preview|restore_contracts':'formats',
  'restore_service|identity':'matches via',
  'restore_service|geometry_utils':'compares geom via',
  'restore_service|serialization':'maps attrs via',
  'restore_service|audit_field_policy':'ignores fields per',
  'restore_service|support_policy':'checks via',
  'restore_executor|restore_service':'calls primitives of',
  'restore_executor|restore_contracts':'consumes',
  'restore_executor|geometry_utils':'applies geom via',
  'rewind_dedup|restore_contracts':'outputs',
  'rewind_dedup|audit_backend':'filters events of',
  'geometry_preview|geometry_utils':'renders via',
  /* read */
  'search_service|audit_backend':'queries types of',
  'search_service|datasource_alias':'resolves via',
  'search_service|identity':'filters by',
  'event_stream_repository|audit_backend':'returns events of',
  'journal_audit|audit_backend':'aggregates',
  /* storage */
  'sqlite_backend|write_queue':'delegates writes to',
  'sqlite_backend|search_service':'delegates reads to',
  'sqlite_backend|sqlite_schema':'follows',
  'sqlite_backend|audit_backend':'implements',
  'journal_manager|sqlite_schema':'creates with',
  'datasource_alias|identity':'linked by',
  'datasource_registry|identity':'indexed by',
  /* infra */
  'observability|logger':'emits via'
};

function initArchGraph(){
  var wrap = document.getElementById('arch-wrap');
  if(!wrap) return;
  var svg = wrap.querySelector('.arch-svg');
  if(!svg) return;
  var titleEl = wrap.querySelector('#arch-title');
  var clusterEl = wrap.querySelector('#arch-cluster-label');
  var descEl = wrap.querySelector('#arch-desc');
  var linesEl = wrap.querySelector('#arch-lines');
  var depsEl = wrap.querySelector('#arch-deps');
  var NS = 'http://www.w3.org/2000/svg';

  function verbFor(from, to){ return ARCH_VERBS[from + '|' + to] || 'uses'; }

  // Build cluster halos + labels
  var haloGroup = document.createElementNS(NS,'g');
  haloGroup.setAttribute('class','arch-cluster-halos');
  ARCH_DATA.clusters.forEach(function(c){
    var rect = document.createElementNS(NS,'rect');
    rect.setAttribute('class','arch-cluster-halo');
    rect.setAttribute('x',c.x); rect.setAttribute('y',c.y);
    rect.setAttribute('width',c.w); rect.setAttribute('height',c.h);
    rect.setAttribute('rx',16); rect.setAttribute('ry',16);
    haloGroup.appendChild(rect);
    var lbl = document.createElementNS(NS,'text');
    lbl.setAttribute('class','arch-cluster-label');
    lbl.setAttribute('x', c.x + 14);
    lbl.setAttribute('y', c.y + 18);
    lbl.textContent = c.label;
    haloGroup.appendChild(lbl);
  });
  svg.appendChild(haloGroup);

  // Index nodes
  var nodeIndex = {};
  ARCH_DATA.nodes.forEach(function(n){ nodeIndex[n.id] = n; });

  // Build edges from deps. Each edge carries its verb and segment trimmed
  // to the producer's circle border so the arrow marker is not hidden.
  var edges = [];
  ARCH_DATA.nodes.forEach(function(n){
    (n.deps||[]).forEach(function(depId){
      var t = nodeIndex[depId];
      if(!t) return;
      var dx = t.x - n.x, dy = t.y - n.y;
      var dist = Math.sqrt(dx*dx + dy*dy) || 1;
      var ux = dx / dist, uy = dy / dist;
      var x1 = n.x + ux * (n.r + 1);
      var y1 = n.y + uy * (n.r + 1);
      var x2 = t.x - ux * (t.r + 4);
      var y2 = t.y - uy * (t.r + 4);
      edges.push({from:n.id, to:depId, fromN:n, toN:t,
                  x1:x1, y1:y1, x2:x2, y2:y2,
                  via: verbFor(n.id, depId)});
    });
  });

  var edgeGroup = document.createElementNS(NS,'g');
  edgeGroup.setAttribute('class','arch-edges');
  var edgeIndex = {};
  edges.forEach(function(e, idx){
    var key = e.from + '\u2192' + e.to;
    edgeIndex[key] = idx;
    var line = document.createElementNS(NS,'line');
    line.setAttribute('class','arch-edge');
    line.setAttribute('data-from', e.from);
    line.setAttribute('data-to', e.to);
    line.setAttribute('data-key', key);
    line.setAttribute('x1', e.x1); line.setAttribute('y1', e.y1);
    line.setAttribute('x2', e.x2); line.setAttribute('y2', e.y2);
    edgeGroup.appendChild(line);
  });
  svg.appendChild(edgeGroup);

  // Hit-area group : invisible thick lines that capture mouse events for
  // edge-level focus. Placed after edges, before nodes, so node hover wins
  // over edge hover when both are pointed.
  var hitGroup = document.createElementNS(NS,'g');
  hitGroup.setAttribute('class','arch-edge-hits');
  edges.forEach(function(e){
    var hit = document.createElementNS(NS,'line');
    hit.setAttribute('class','arch-edge-hit');
    hit.setAttribute('data-from', e.from);
    hit.setAttribute('data-to', e.to);
    hit.setAttribute('data-key', e.from + '\u2192' + e.to);
    hit.setAttribute('x1', e.x1); hit.setAttribute('y1', e.y1);
    hit.setAttribute('x2', e.x2); hit.setAttribute('y2', e.y2);
    hitGroup.appendChild(hit);
  });
  svg.appendChild(hitGroup);

  // Edge-label group : each label is a horizontal badge placed along its
  // segment. To avoid badges piling up at the same midpoint, a spatial
  // grid resolves collisions by sliding each label along its own line
  // between t=0.30 and t=0.70. No rotation : text stays readable.
  function labelBox(e, t, w, h){
    var cx = e.x1 + (e.x2 - e.x1) * t;
    var cy = e.y1 + (e.y2 - e.y1) * t;
    return {cx:cx, cy:cy, x:cx - w/2, y:cy - h/2, w:w, h:h, t:t};
  }
  function boxesOverlap(a, b){
    var pad = 4;
    return !(a.x + a.w + pad < b.x || b.x + b.w + pad < a.x ||
             a.y + a.h + pad < b.y || b.y + b.h + pad < a.y);
  }
  var grid = {};
  var cellSize = 64;
  function cellsFor(box){
    var c0 = Math.floor(box.x / cellSize), c1 = Math.floor((box.x + box.w) / cellSize);
    var r0 = Math.floor(box.y / cellSize), r1 = Math.floor((box.y + box.h) / cellSize);
    var keys = [];
    for(var c = c0; c <= c1; c++){
      for(var r = r0; r <= r1; r++){ keys.push(c + ',' + r); }
    }
    return keys;
  }
  function gridHasCollision(box){
    var keys = cellsFor(box);
    for(var i = 0; i < keys.length; i++){
      var bucket = grid[keys[i]];
      if(!bucket) continue;
      for(var j = 0; j < bucket.length; j++){
        if(boxesOverlap(box, bucket[j])) return true;
      }
    }
    return false;
  }
  function gridPush(box){
    cellsFor(box).forEach(function(k){ (grid[k] = grid[k] || []).push(box); });
  }
  // Try positions in this order : center first, then alternating outward.
  var T_CANDIDATES = [0.50, 0.42, 0.58, 0.34, 0.66, 0.30, 0.70];

  var labelGroup = document.createElementNS(NS,'g');
  labelGroup.setAttribute('class','arch-edge-labels');
  edges.forEach(function(e){
    var w = Math.max(44, e.via.length * 7.4 + 18);
    var h = 22;
    var chosen = null;
    for(var i = 0; i < T_CANDIDATES.length; i++){
      var box = labelBox(e, T_CANDIDATES[i], w, h);
      if(!gridHasCollision(box)){ chosen = box; break; }
    }
    if(!chosen){ chosen = labelBox(e, 0.50, w, h); } // fallback : accept overlap
    gridPush(chosen);
    e.labelT = chosen.t; // remember for diagnostics

    var g = document.createElementNS(NS,'g');
    g.setAttribute('class','arch-edge-label');
    g.setAttribute('data-from', e.from);
    g.setAttribute('data-to', e.to);
    g.setAttribute('data-key', e.from + '\u2192' + e.to);
    g.setAttribute('transform','translate(' + chosen.cx + ',' + chosen.cy + ')');
    var bg = document.createElementNS(NS,'rect');
    bg.setAttribute('class','arch-edge-label-bg');
    bg.setAttribute('x', -w/2);
    bg.setAttribute('y', -h/2);
    bg.setAttribute('width', w);
    bg.setAttribute('height', h);
    bg.setAttribute('rx', 4);
    g.appendChild(bg);
    var t = document.createElementNS(NS,'text');
    t.setAttribute('class','arch-edge-label-text');
    t.setAttribute('text-anchor','middle');
    t.setAttribute('y', 4);
    t.textContent = e.via;
    g.appendChild(t);
    labelGroup.appendChild(g);
  });
  /* labelGroup is appended AFTER nodes and arrows so badges sit on top */

  // Build nodes
  var nodeGroup = document.createElementNS(NS,'g');
  nodeGroup.setAttribute('class','arch-nodes');
  ARCH_DATA.nodes.forEach(function(n){
    var g = document.createElementNS(NS,'g');
    g.setAttribute('class','arch-node');
    g.setAttribute('data-id', n.id);
    g.setAttribute('data-cluster', n.cluster);

    // Glow ring (under circle, dashed)
    var glow = document.createElementNS(NS,'circle');
    glow.setAttribute('class','arch-node-glow');
    glow.setAttribute('cx',n.x); glow.setAttribute('cy',n.y);
    glow.setAttribute('r', n.r + 11);
    glow.setAttribute('fill','none');
    glow.setAttribute('stroke','var(--ac)');
    glow.setAttribute('stroke-width','1');
    glow.setAttribute('stroke-dasharray','2 4');
    g.appendChild(glow);

    // Main circle
    var c = document.createElementNS(NS,'circle');
    c.setAttribute('cx',n.x); c.setAttribute('cy',n.y); c.setAttribute('r',n.r);
    g.appendChild(c);

    // Label below
    var t = document.createElementNS(NS,'text');
    t.setAttribute('x',n.x);
    t.setAttribute('y', n.y + n.r + 14);
    t.textContent = n.label;
    g.appendChild(t);

    nodeGroup.appendChild(g);
  });
  svg.appendChild(nodeGroup);

  // Arrow group : directional triangles rendered AFTER nodes so they are
  // never hidden by the target circle. Positioned just outside the target
  // node, oriented along the segment. Both offsets are kept equal now that
  // the active node has no halo glow; the dynamic re-place hook stays in
  // place so a halo could be reintroduced later without code change.
  var ARROW_OFFSET_REST = 9;
  var ARROW_OFFSET_ACTIVE_TO = 9;
  function placeArrow(poly, offset){
    var tx = parseFloat(poly.getAttribute('data-tx'));
    var ty = parseFloat(poly.getAttribute('data-ty'));
    var r  = parseFloat(poly.getAttribute('data-r'));
    var ux = parseFloat(poly.getAttribute('data-ux'));
    var uy = parseFloat(poly.getAttribute('data-uy'));
    var ang= parseFloat(poly.getAttribute('data-ang'));
    var px = tx - ux * (r + offset);
    var py = ty - uy * (r + offset);
    poly.setAttribute('transform','translate(' + px + ',' + py + ') rotate(' + ang + ')');
  }
  var arrowGroup = document.createElementNS(NS,'g');
  arrowGroup.setAttribute('class','arch-edge-arrows');
  edges.forEach(function(e){
    var dx = e.toN.x - e.fromN.x;
    var dy = e.toN.y - e.fromN.y;
    var dist = Math.sqrt(dx*dx + dy*dy) || 1;
    var ux = dx / dist, uy = dy / dist;
    var ang = Math.atan2(dy, dx) * 180 / Math.PI;
    var a = document.createElementNS(NS,'polygon');
    a.setAttribute('class','arch-edge-arrow');
    a.setAttribute('data-from', e.from);
    a.setAttribute('data-key', e.from + '\u2192' + e.to);
    a.setAttribute('data-to', e.to);
    a.setAttribute('data-tx', e.toN.x);
    a.setAttribute('data-ty', e.toN.y);
    a.setAttribute('data-r',  e.toN.r);
    a.setAttribute('data-ux', ux);
    a.setAttribute('data-uy', uy);
    a.setAttribute('data-ang', ang);
    a.setAttribute('points','0,-5 9,0 0,5');
    placeArrow(a, ARROW_OFFSET_REST);
    arrowGroup.appendChild(a);
  });
  svg.appendChild(arrowGroup);

  // Now the labels, ordered last so badges stay readable on top
  svg.appendChild(labelGroup);

  // State
  var typewriter = null;
  var activeId = null;
  var focusedEdgeKey = null;

  function findReverseDeps(id){
    var out = [];
    ARCH_DATA.nodes.forEach(function(n){
      if((n.deps||[]).indexOf(id) >= 0) out.push(n.id);
    });
    return out;
  }

  // Edge-level focus : isolate ONE edge among the connected ones. The active
  // node still drives the description panel; this just emphasises one line.
  function focusEdge(key){
    if(focusedEdgeKey === key) return;
    focusedEdgeKey = key;
    svg.classList.add('has-edge-focus');
    svg.querySelectorAll('.arch-edge.edge-focus, .arch-edge-arrow.edge-focus, .arch-edge-label.edge-focus')
       .forEach(function(el){ el.classList.remove('edge-focus'); });
    svg.querySelectorAll('[data-key="'+key+'"]').forEach(function(el){
      if(el.classList.contains('arch-edge') || el.classList.contains('arch-edge-arrow') || el.classList.contains('arch-edge-label')){
        el.classList.add('edge-focus');
      }
    });
  }
  function clearEdgeFocus(){
    if(!focusedEdgeKey) return;
    focusedEdgeKey = null;
    svg.classList.remove('has-edge-focus');
    svg.querySelectorAll('.edge-focus').forEach(function(el){ el.classList.remove('edge-focus'); });
  }

  function activate(id){
    if(activeId === id) return;
    activeId = id;
    var node = nodeIndex[id];
    if(!node) return;

    clearEdgeFocus();
    svg.classList.add('has-active');
    svg.querySelectorAll('.arch-node').forEach(function(g){ g.classList.remove('active','connected'); });
    svg.querySelectorAll('.arch-edge').forEach(function(e){ e.classList.remove('connected'); });
    svg.querySelectorAll('.arch-edge-arrow').forEach(function(a){ a.classList.remove('connected'); });
    svg.querySelectorAll('.arch-edge-label').forEach(function(g){ g.classList.remove('connected'); });

    var ag = svg.querySelector('.arch-node[data-id="'+id+'"]');
    if(ag) ag.classList.add('active');

    var connected = {};
    (node.deps||[]).forEach(function(d){ connected[d] = true; });
    findReverseDeps(id).forEach(function(d){ connected[d] = true; });
    Object.keys(connected).forEach(function(cid){
      var g = svg.querySelector('.arch-node[data-id="'+cid+'"]');
      if(g) g.classList.add('connected');
    });
    svg.querySelectorAll('.arch-edge[data-from="'+id+'"], .arch-edge[data-to="'+id+'"]').forEach(function(e){ e.classList.add('connected'); });
    svg.querySelectorAll('.arch-edge-arrow[data-from="'+id+'"], .arch-edge-arrow[data-to="'+id+'"]').forEach(function(a){ a.classList.add('connected'); });
    // Reset all arrows to rest offset, then push incoming arrows further out so
    // they clear the active node's drop-shadow halo and stay perfectly on the line.
    svg.querySelectorAll('.arch-edge-arrow').forEach(function(a){ placeArrow(a, ARROW_OFFSET_REST); });
    svg.querySelectorAll('.arch-edge-arrow[data-to="'+id+'"]').forEach(function(a){ placeArrow(a, ARROW_OFFSET_ACTIVE_TO); });
    svg.querySelectorAll('.arch-edge-label[data-from="'+id+'"], .arch-edge-label[data-to="'+id+'"]').forEach(function(g){ g.classList.add('connected'); });

    titleEl.textContent = node.label;
    var clusterMeta = ARCH_DATA.clusters.filter(function(c){return c.id===node.cluster;})[0];
    clusterEl.textContent = clusterMeta ? clusterMeta.label : node.cluster;
    linesEl.innerHTML = '<strong>' + node.lines + '</strong> lines';

    if(typewriter) clearInterval(typewriter);
    descEl.classList.remove('done');
    descEl.textContent = '';
    var i = 0;
    var txt = node.desc;
    typewriter = setInterval(function(){
      if(i >= txt.length){
        clearInterval(typewriter); typewriter = null;
        descEl.classList.add('done');
        return;
      }
      descEl.textContent += txt.charAt(i++);
    }, 9);

    depsEl.innerHTML = '';
    var seen = {};
    (node.deps||[]).forEach(function(depId){
      if(seen[depId]) return; seen[depId] = true;
      var t = nodeIndex[depId]; if(!t) return;
      var chip = document.createElement('span');
      chip.className = 'arch-detail-dep';
      var fv = verbFor(node.id, depId);
      chip.innerHTML = '<span class="arch-chip-arrow">\u2192</span> <span class="arch-chip-verb">' + fv + '</span> ' + t.label;
      chip.dataset.id = depId;
      chip.title = node.label + ' ' + fv + ' ' + t.label;
      chip.addEventListener('mouseenter', function(){ activate(depId); });
      depsEl.appendChild(chip);
    });
    findReverseDeps(id).forEach(function(rid){
      if(seen[rid]) return; seen[rid] = true;
      var t = nodeIndex[rid]; if(!t) return;
      var chip = document.createElement('span');
      chip.className = 'arch-detail-dep';
      var rv = verbFor(rid, id);
      chip.innerHTML = '<span class="arch-chip-arrow">\u2190</span> ' + t.label + ' <span class="arch-chip-verb">' + rv + '</span>';
      chip.dataset.id = rid;
      chip.title = t.label + ' ' + rv + ' ' + node.label;
      chip.addEventListener('mouseenter', function(){ activate(rid); });
      depsEl.appendChild(chip);
    });
  }

  function deactivate(){
    if(!activeId) return;
    activeId = null;
    clearEdgeFocus();
    svg.classList.remove('has-active');
    svg.querySelectorAll('.arch-node').forEach(function(g){ g.classList.remove('active','connected'); });
    svg.querySelectorAll('.arch-edge').forEach(function(e){ e.classList.remove('connected'); });
    svg.querySelectorAll('.arch-edge-arrow').forEach(function(a){ a.classList.remove('connected'); placeArrow(a, ARROW_OFFSET_REST); });
    svg.querySelectorAll('.arch-edge-label').forEach(function(g){ g.classList.remove('connected'); });
    if(typewriter){ clearInterval(typewriter); typewriter = null; }
    titleEl.textContent = 'Hover a module to explore';
    clusterEl.textContent = ARCH_DATA.nodes.length + ' modules \u00b7 ' + edges.length + ' dependencies';
    descEl.classList.add('done');
    descEl.textContent = '';
    linesEl.innerHTML = '\u2014';
    depsEl.innerHTML = '<span class="arch-idle-tip"><span class="arch-idle-pulse"></span>Move your cursor over any node. The graph fades everything else and streams a description. Click a chip below to jump.</span>';
  }

  // Wire node events
  svg.querySelectorAll('.arch-node').forEach(function(g){
    g.addEventListener('mouseenter', function(){
      clearEdgeFocus();
      activate(g.dataset.id);
    });
    g.addEventListener('focus', function(){ activate(g.dataset.id); });
    g.setAttribute('tabindex', '0');
  });

  // Wire edge hit-area events : entering an edge focuses on it AND on its
  // target node (keeps the description panel filled). Leaving the edge only
  // clears the edge-level focus, keeping the node context.
  svg.querySelectorAll('.arch-edge-hit').forEach(function(hit){
    hit.addEventListener('mouseenter', function(){
      var key = hit.dataset.key;
      var to = hit.dataset.to;
      activate(to);
      focusEdge(key);
    });
    hit.addEventListener('mouseleave', function(){ clearEdgeFocus(); });
  });

  // Mouseleave on the wrapper resets graph
  wrap.addEventListener('mouseleave', function(){
    clearEdgeFocus();
    deactivate();
  });

  // Cluster legend filtering: hover legend pill highlights first node of cluster
  wrap.querySelectorAll('.arch-leg').forEach(function(leg){
    leg.addEventListener('mouseenter', function(){
      var cluster = leg.dataset.cluster;
      var first = ARCH_DATA.nodes.filter(function(n){return n.cluster===cluster;})[0];
      if(first) activate(first.id);
    });
  });

  // Initial state
  deactivate();
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
  initArchGraph();
});
