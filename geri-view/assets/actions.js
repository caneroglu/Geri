// Her riskli düğme kendi timer/pointer/payload durumunu taşır; Datastar yalnız komutu gönderir.
(() => {
  if (customElements.get('geri-action')) return;
  class GeriAction extends HTMLElement {
    #active = null;
    #generation = 0;
    #listeners;
    #observer;

    connectedCallback() {
      this.#listeners = new AbortController();
      window.addEventListener('blur', () => this.cancel(), {signal: this.#listeners.signal});
      document.addEventListener('visibilitychange', () => {
        if (document.hidden) this.cancel();
      }, {signal: this.#listeners.signal});
      this.#observer = new MutationObserver(() => {
        if (this.#active && !this.#valid(this.#active)) this.cancel();
      });
      this.#observer.observe(this, {subtree: true, childList: true, attributes: true,
        attributeFilter: ['disabled', 'data-danger', 'data-delay', 'data-on:click',
          'data-on:pointerdown', 'data-on:geri-confirm']});
    }

    disconnectedCallback() {
      this.cancel();
      this.#listeners?.abort();
      this.#observer?.disconnect();
    }

    arm(event, payload) {
      const button = event.currentTarget;
      const hold = this.dataset.danger === 'destructive';
      if (!this.isConnected || !this.contains(button) || button.disabled ||
          event.button !== 0 || (hold && !event.isPrimary)) return;
      // Başka pointer'ın tekrarı mevcut basışı bozmaz.
      if (hold && this.#active && this.#active.pointerId !== event.pointerId) return;
      this.cancel();
      const generation = this.#generation;
      const active = {
        generation, button, hold, pointerId: hold ? event.pointerId : null,
        payload: JSON.parse(JSON.stringify(payload)),
        danger: this.dataset.danger, delay: this.dataset.delay,
        confirm: this.getAttribute('data-on:geri-confirm'),
        arm: button.getAttribute(hold ? 'data-on:pointerdown' : 'data-on:click'),
        listeners: new AbortController(),
      };
      this.#active = active;
      if (hold) {
        event.preventDefault();
        button.classList.add('holding');
        for (const type of ['pointerup', 'pointercancel']) {
          window.addEventListener(type, e => this.release(e), {capture: true, signal: active.listeners.signal});
        }
        window.addEventListener('pointermove', e => {
          if (e.pointerId !== active.pointerId) return;
          const r = button.getBoundingClientRect();
          if (!(e.buttons & 1) || e.clientX < r.left || e.clientX > r.right ||
              e.clientY < r.top || e.clientY > r.bottom) this.cancel();
        }, {capture: true, signal: active.listeners.signal});
        button.addEventListener('lostpointercapture', e => this.release(e), {signal: active.listeners.signal});
        // Sentetik olaylarda gerçek bir aktif pointer bulunmayabilir.
        try { button.setPointerCapture(event.pointerId); } catch {}
      } else {
        button.style.display = 'none';
        this.querySelector('.undo').style.display = '';
      }
      active.timer = setTimeout(() => {
        if (this.#active !== active || this.#generation !== generation) return;
        if (!this.#valid(active)) { this.cancel(); return; }
        const snapshot = active.payload;
        this.cancel();
        this.dispatchEvent(new CustomEvent('geri-confirm', {detail: snapshot, bubbles: true}));
      }, Number(active.delay));
    }

    release(event) {
      if (this.#active?.hold && this.#active.pointerId === event.pointerId) this.cancel();
    }

    cancel() {
      const active = this.#active;
      this.#active = null;
      ++this.#generation;
      if (!active) return;
      clearTimeout(active.timer);
      active.listeners.abort();
      active.button.classList.remove('holding');
      if (active.hold) {
        if (active.button.hasPointerCapture(active.pointerId)) active.button.releasePointerCapture(active.pointerId);
      } else {
        active.button.style.display = '';
        const undo = this.querySelector('.undo');
        if (undo) undo.style.display = 'none';
      }
    }

    #valid(active) {
      const visible = active.hold ? active.button : this.querySelector('.undo');
      return this.isConnected && this.contains(active.button) && !active.button.disabled &&
        !!visible?.getClientRects().length &&
        !this.closest('.ctl.locked') && this.dataset.danger === active.danger &&
        this.dataset.delay === active.delay && this.getAttribute('data-on:geri-confirm') === active.confirm &&
        active.button.getAttribute(active.hold ? 'data-on:pointerdown' : 'data-on:click') === active.arm;
    }
  }
  customElements.define('geri-action', GeriAction);
})();
