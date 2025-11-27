const fs = require('fs');
const path = require('path');

async function loadHandlers(pool) {
  const handlers = {};
  const dir = __dirname;
  const files = fs.readdirSync(dir).filter((f) => f !== 'index.js' && f.endsWith('.js'));

  for (const file of files) {
    const modPath = path.join(dir, file);
    try {
      const h = require(modPath);
      if (h && h.messageType) {
        const names = Array.isArray(h.messageType) ? h.messageType : [h.messageType];
        for (const name of names) {
          if (handlers[name]) {
            console.warn(`Handler for messageType ${name} is being overwritten by ${modPath}`);
          }
          handlers[name] = h;
        }
        if (pool && typeof h.ensure === 'function') {
          try {
            await h.ensure(pool);
            console.log(`Ensured table for handler ${names.join(',')}`);
          } catch (err) {
            console.error(`Failed to ensure table for handler ${names.join(',')}:`, err);
          }
        }
      }
    } catch (err) {
      console.error('Failed to load handler', modPath, err);
    }
  }

  return handlers;
}

module.exports = { loadHandlers };
