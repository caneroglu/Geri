// Çok satırlı kod kutusu: textarea saydam, arkasındaki <pre> GeriHL ile boyanır. İkisi aynı grid
// hücresinde üst üste durur ve aynı satır kırmasını kullanır; kutu içerikle uzar, kaydırma sarmalayıcıda.
(() => {
  if (customElements.get('geri-code')) return;
  const esc = s => s.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
  class GeriCode extends HTMLElement {
    #listeners;
    #painted = null;
    #frame = 0;

    connectedCallback() {
      this.#listeners = new AbortController();
      const signal = this.#listeners.signal;
      this.addEventListener('input', () => this.paint(), {signal});
      // Datastar değeri sinyalden, olaydan sonra ve input atmadan yazar; bir kare sonra boya.
      document.addEventListener('datastar-signal-patch', () => this.schedule(), {signal});
      this.schedule();
    }

    disconnectedCallback() {
      this.#listeners?.abort();
      cancelAnimationFrame(this.#frame);
      this.#frame = 0;
    }

    schedule() {
      if (this.#frame) return;
      this.#frame = requestAnimationFrame(() => {
        this.#frame = 0;
        this.paint();
      });
    }

    paint() {
      const area = this.querySelector('textarea');
      const code = this.querySelector('pre > code');
      if (!area || !code || area.value === this.#painted) return;
      const text = this.#painted = area.value;
      const hl = globalThis.GeriHL;
      const lang = this.dataset.lang;
      let html = '';
      if (hl && lang in hl.languages) {
        hl.tokenizeWith(text, lang, (t, type) => {
          html += type ? `<span class="hl-${type}">${esc(t)}</span>` : esc(t);
        }, {languages: hl.languages});
      } else {
        html = esc(text);
      }
      // Son satır boşsa da yükseklik tutsun.
      code.innerHTML = html + '\n';
    }
  }
  customElements.define('geri-code', GeriCode);
})();
