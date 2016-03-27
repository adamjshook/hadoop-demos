function MetaDataModel() {
  var self = this;
  self.rows = ko.observableArray();
  self.load = function (data) {
    //first clear whats there
    self.rows.removeAll();
    for (var i = 0; i < data.length; i++) {
      var row = new TweetRow(data[i]);
      self.rows.push(row);
    };
  };
};

function TweetRow(data) {
  var self = this;
  self.id = data.id;
  self.created = toTimestampStr(new Date(data.created));
  self.userId = data.userId;
  self.tweet = data.tweet;
};

function toTimestampStr(date) {
  return pad((date.getMonth() + 1)) + "/" + pad(date.getDate()) + "/" + date.getFullYear() + " " + pad(date.getHours()) + ":" + pad(date.getMinutes()) + ":" + pad(date.getSeconds());
}

function pad(number) {
  return Array(Math.max(2 - String(number).length + 1, 0)).join(0) + number;
}

function LookupModel() {
  var self = this;
  self.word = ko.observable();
}

function HashTagModel() {
  var self = this;
  self.rows = ko.observableArray();
  self.load = function (data) {
    //first clear whats there
    self.rows.removeAll();
    for (var i = 0; i < data.length; i++) {
      var row = new HashTag(data[i]);
      self.rows.push(row);
    };
  };
};

function HashTag(data) {
  var self = this;
  self.tag = data;
}

function ApplicationModel() {
  var self = this;
  self.metaDataModel = ko.observable(new MetaDataModel());
  self.lookupModel = ko.observable(new LookupModel());
  self.hashtagModel = ko.observable(new HashTagModel());

  self.hashtags = function () {
    $.getJSON("/hashtags", function (data) {
      self.hashtagModel().load(data);
    });
    setTimeout(self.hashtags, 5000);
  }

  self.start = function () {
    self.hashtags();
  };

  self.lookup = function () {
    var data = "";
    $.getJSON("/lookup?" + data, function (data) {
      self.metaDataModel().load(data);
    });
  };

}