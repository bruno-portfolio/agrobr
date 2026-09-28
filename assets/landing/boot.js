const stage = document.getElementById('terrainStage');
const retry = document.getElementById('terrainRetry');
let attempts = 0;

async function loadExplorer() {
  retry.hidden = true;
  stage.dataset.state = 'idle';
  try {
    const suffix = attempts ? '?retry=' + attempts : '';
    attempts += 1;
    await import('/assets/landing/explorer.js' + suffix);
  } catch {
    stage.dataset.state = 'error';
    document.getElementById('terrainLoading').hidden = false;
    document.getElementById('terrainLoadingText').textContent = document.documentElement.lang.startsWith('en')
      ? 'The explorer could not load. Check your connection and try again.'
      : 'O explorer não carregou. Confira a conexão e tente novamente.';
    retry.hidden = false;
    retry.addEventListener('click', loadExplorer, { once: true });
  }
}

const observer = new IntersectionObserver(entries => {
  if (entries.some(entry => entry.isIntersecting)) {
    observer.disconnect();
    loadExplorer();
  }
}, { rootMargin: '250px' });
observer.observe(stage);
