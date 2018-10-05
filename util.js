'use strict';

(function() {
  const argv = require('minimist')(process.argv.slice(2));

  module.exports = {
    getConfig: function getConfig(name, defaultValue) {
      const config = argv[name] || argv[name.toLowerCase()] || process.env[name] || null;

      if (typeof defaultValue === 'undefined' && config === null) {
        throw new Error('You must specify ' + name);
      }

      return config || defaultValue;
    }
  };

}).call(this);
