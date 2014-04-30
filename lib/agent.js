var fs   = require('fs'),
    exec = require('child_process').exec,
    path = require('path'),
    os   = require('os'),
    util = require('util'),
    yaml = require('js-yaml'),
    moment = require('moment'),
    redis = require("redis"),
    _     = require("underscore"),
    async = require("async");

function Agent(config){
  if(!_.isObject(config)){
    return console.error("Bad config");
  }

  this.nutcracker_config_file = config.nutcracker_config_file;
  this.redis_sentinel_ip      = config.redis_sentinel_ip;
  this.redis_sentinel_port    = config.redis_sentinel_port;
  this.restart_command        = config.restart_command;
  this.conn_retry_count       = 0;
  this.log_file               = config.log_file;
  this.rewrite_cargo          = async.cargo(this.switch_master.bind(this));
}

// Logs a message to the console and to the file
// specifid in the cli.js
Agent.prototype.log = function (message) {
  var theMessage = util.format("[%s] %s", moment().format(), message);
  util.puts(theMessage);

  if(this.log_file != undefined) {
     fs.appendFile(this.log_file, theMessage + '\n', function(err) {

     });
  };
};

// Restarts TwemProxy
Agent.prototype.restart_twemproxy = function(callback){
  var self = this;
  var child = exec(
    this.restart_command,
    function(error, stdout, stderr) {
      self.log("TwemProxy restarted with output: ");
      self.log(stdout);
      if (error !== null) {
        self.log("TwemProxy failed restarting with error: " + error);
      }

      return callback();
    }
  );
};

// Updates the address of a server, by its name, in the TwemProxy config
Agent.prototype.switch_master = function(tasks, callback){
  var self = this;
  self.log("Received switch-master: " + util.inspect(tasks));

  tasks.forEach(function(task) {
    self.log("Updating Master " + task.server + " to " + task.address);
    var found = false,
        new_value = util.format("%s:1 %s", task.address, task.server);
    _.each(self.doc, function(proxy_data, proxy_name) {
      _.each(proxy_data.servers, function(server_entry, server_idx) {
        // we need to get the server name from the config value
        var conf_name = _.last(server_entry.split(' '));
        if(conf_name == task.server) {
          // We've found the matching server
          proxy_data.servers[server_idx] = new_value;
          found = true;
        };
      });
    });
    if (!found) {
      self.log("WARNING: Update Failed! Server " + task.server + " not found in TwemProxy config!");
    }
  });

  async.series([
    function(callback) { self.save_twemproxy_config(callback); },
    function(callback) { self.restart_twemproxy(callback); }
  ], callback);
};

// Loads the TwemProxy config file from disk
Agent.prototype.load_twemproxy_config = function(callback){
  this.log("Loading TwemProxy config");
  try {
    this.doc = yaml.safeLoad(fs.readFileSync(this.nutcracker_config_file, 'utf8'));
    callback();
  } catch (e) {
    return callback(e);
  }
};

// Saves the TwemProxy config file to disk
Agent.prototype.save_twemproxy_config = function(callback){
  this.log("Saving TwemProxy config");
  fs.writeFile(this.nutcracker_config_file, yaml.safeDump(this.doc), callback);
};

// This will connect to Redis Sentinel and get a list of all current
// master servers, and ensure our config is full up to date
Agent.prototype.on_connect = function() {
  var self = this;
  this.log("Connection to Redis Sentinel established.")
  this.log("Getting latest list of masters...");

  // Get the masters list
  this.client.send_command("SENTINEL", ["masters"], function (err, reply) {
    var changes = [];

    for (var i = 0; i < reply.length; i++) {
      var server = reply[i][1];
      var address = reply[i][3] + ":" + reply[i][5];

      self.log("Master received: " + server + " " + address);
      changes.push({server: server, address: address});
    }

    self.rewrite_cargo.push(changes);

    self.log("Subscribing to sentinel.");

    self.client.on("pmessage", function (p, ch, msg) {
      var parts = msg.split(' ');
      self.rewrite_cargo.push({server: parts[0], address: util.format("%s:%s", parts[3], parts[4])});
    });

    self.client.psubscribe('+switch-master');
  });
};

// Starts the pub/sub monitor on Sentinel
Agent.prototype.start_sentinel = function(){

  this.log("Redis Sentinel TwemProxy Agent Started");
  this.log(util.format("Connecting to sentinel on redis://%s:%s", this.redis_sentinel_ip, this.redis_sentinel_port));
  var self = this;
  this.client = redis.createClient(
      this.redis_sentinel_port,
      this.redis_sentinel_ip,
      {
         retry_max_delay: 5000
      }
    );

  this.client.on("error", function(msg) {
    if (msg.toString().indexOf("ECONNREFUSED") == -1) {
      self.log("Redis TwemProxy Agent encountered an error: ");
      self.log(msg);
    } else {
      self.conn_retry_count = self.conn_retry_count + 1;
      if (self.conn_retry_count % 10 == 0) {
        self.log("WARNING: Connection to Redis Sentinel has failed " + self.conn_retry_count + " times!");
      };
    };
  });

  this.client.on("end", function() {
    self.log("Error: Connection to Redis Sentinel was closed!");
  });

  this.client.on("connect", function() {
    self.on_connect();
  });
};

// Initialisation
Agent.prototype.bootstrap = function(){
  var self = this;

  this.load_twemproxy_config(function(error) {
    if(error) {
      return self.log(error);
    }

    return self.start_sentinel();
  });
};

// Initialisation
Agent.bootstrap = function (config) {
  (new Agent(config)).bootstrap();
};

module.exports = Agent;
