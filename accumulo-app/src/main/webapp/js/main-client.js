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

function ApplicationModel() {
  var self = this;
  self.metaDataModel = ko.observable(new MetaDataModel());
  self.lookupModel = ko.observable(new LookupModel());

  self.start = function () {

  };

  self.lookup = function () {
    var data = "";
    data += 'word=' + self.lookupModel().word();
    console.log(data);
    $.getJSON("/lookup?" + data, function (data) {
      self.metaDataModel().load(data);
    });
  };
}