
App.prototype.createGraphDiv = function(key) {

  if (key != undefined) {
    $("#results").append($('<div class="row" id="graphDivRow' + key + '"><div class="text-center" id="graphDiv' + key + '"></div></div>"'));
  } else {
    $("#results").append($('<div class="row" id="graphDivRow"><div class="text-center" id="graphDiv"></div></div>'));
    $("#results").append($('<div class="row" id="legendDivRow"><div class="text-center" id="legendDiv"></div></div>'));
  }
}

App.prototype.removeGraphDivs = function() {
  $("div").each(function () {
    if (this.id.indexOf("graphDiv") >= 0) {
      $(this).remove();
    } else if (this.id.indexOf("legendDiv") >= 0) {
      $(this).remove();
    }
  })
}

App.prototype.showGraphs = function() {

  this.mode = "graph";

  if (selection = this.getFormData(true)) {

    this.hideMap();

    this.removeGraphDivs();

    this.startLoadingAnimation();

    var user = this.data.filter(function(value) { return value.key == selection.user })[0].values;
    var userRows = user.filter(function(value) { return value.key == selection.day })[0].values;

    var self = this;

    var promises = [];
    var availableFileNames = [];

    if ($("#filesMode option:selected").val() == "one") {

      var divCount = 0;

      userRows.forEach(function(row, key) {
        if ($.inArray(row.fileTypeName, [self.mapFileTypeName]) == -1 && $.inArray(row.fileTypeName, $("#files").val()) != -1) {
          availableFileNames.push(row.fileTypeName);
          self.createGraphDiv(divCount++);
          promises.push(self.readFile("data/" + selection.user + "_" + row.fileTypeName + "_" + selection.day + ".csv", row.fileTypeName));
        }
      })

    } else {

      self.createGraphDiv();

      userRows.forEach(function(row, key) {
        if ($.inArray(row.fileTypeName, [self.mapFileTypeName]) == -1 && $.inArray(row.fileTypeName, $("#files").val()) != -1) {
          availableFileNames.push(row.fileTypeName);
          promises.push(self.readFile("data/" + selection.user + "_" + row.fileTypeName + "_" + selection.day + ".csv", row.fileTypeName));
        }
      })
    }

    this.drawGraphs(promises, availableFileNames);
  }
}

App.prototype.drawGraphs = function(promises, availableFileNames) {

  var self = this;

  $.when.apply($, promises).then(function() {

    var format = d3.time.format.utc("%b %e, %Y %I:%M%p");

    if ($("#filesMode option:selected").val() == "one") {

      var isAnyDataFound = false;

      for (var i = 0; i < arguments.length; i++) {

        if (arguments[i].data.length > 0 && !isAnyDataFound) {
          isAnyDataFound = true;
        }
    
        if (arguments[i].data.length > 0) {

          var options = {
            title: arguments[i].fileTypeName,
            data: arguments[i].data,
            target: "#graphDiv" + i,
            interpolate: "basic",
            min_y_from_data: true,
            full_width: true,
            height: 200,
            min_y_from_data: true,
            brushing: false,
            missing_is_hidden: true,
            linked: true,
            linked_format: "%Y-%m-%d-%H-%M-%S",
            utc_time: true
          }

          if ($.inArray(arguments[i].fileTypeName, ["name_pageTitle",
"origin",
"http_method",
"name_url",
"http_httpVersionRequest",
"type_header_Accept",
"connection_header_ConnectionRequest",
"connection_header_KeepAliveRequest",
"http_statusText",
"http_httpVersionResponse",
"encoding_header_ContentEncoding",
"connection_header_KeepAliveResponse",
"connection_header_ConnectionResponse",
"type_header_ContentType",
"redirect_redirectUrl",
"type_content_compression",
"type_content_mimeType",
"name_domain",
"name_title",
"name_Category1",
"location_location_address",
"apps",
"connection_networkType",
"connection_roaming",
"network_ssid",
"network_state",
"network_internet",
"network_mobile",
"network_wifi",
"phoneState",
"phoneScreen"]) != -1) {

            var markers = arguments[i].data.map(function(d) {
              return {
                date: d.date,
                label: d.trueValue
              }
            })

            options.markers = markers;

            options.mouseover = function(d) {
              $("#graphDiv" + this.i + " svg .mg-active-datapoint").html(format(d.date) + " " + d.trueValue);
            }.bind({i: i})
          }

          MG.data_graphic(options);

        } //end else if

      } //end for

      if (!isAnyDataFound) {
        notie.alert(4, "No data found", 1);
      }

    } else {

      var data = [];

      var markers = [];

      for(var i = 0; i < arguments.length; i++) {

        if (arguments[i].data.length > 0) {
        
          if ($.inArray(arguments[i].fileTypeName, ["name_pageTitle",
"origin",
"http_method",
"name_url",
"http_httpVersionRequest",
"type_header_Accept",
"connection_header_ConnectionRequest",
"connection_header_KeepAliveRequest",
"http_statusText",
"http_httpVersionResponse",
"encoding_header_ContentEncoding",
"connection_header_KeepAliveResponse",
"connection_header_ConnectionResponse",
"type_header_ContentType",
"redirect_redirectUrl",
"type_content_compression",
"type_content_mimeType",
"name_domain",
"name_title",
"name_Category1",
"location_location_address",
"apps",
"connection_networkType",
"connection_roaming",
"network_ssid",
"network_state",
"network_internet",
"network_mobile",
"network_wifi",
"phoneState",
"phoneScreen"]) != -1) {

            markers = arguments[i].data.map(function(d) {
              return {
                date: d.date,
                label: d.trueValue
              }
            })
          }

          data.push(arguments[i].data);
        }
      }

      function customRollover(d, i) {

        var textContainer = d3.select("#graphDiv svg .mg-active-datapoint"),
          lineCount = 0,
          lineHeight = 1.3,
          textSize = "1.0rem"

        textContainer.selectAll("*").remove();

        var format = d3.time.format.utc("%b %e, %Y %I:%M%p");
        var date = d.key ? format(new Date(d.key)) : format(d.date);

        textContainer.append("tspan")
          .text(date)
          .style("font-size", textSize);

        lineCount = 1;

        if (d.value != undefined) {
        
          var label = textContainer.append("tspan")
            .attr({
                x: 0,
                y: (lineCount * lineHeight) + "em"
            })
            .text(d.trueValue ? d.trueValue : d.value)
            .style("font-size", textSize);

          textContainer.append("tspan")
            .attr({
                x: -label.node().getComputedTextLength(),
                y: (lineCount * lineHeight) + "em"
            })
            .text("\u2014 ") // mdash
            .classed("mg-hover-line" + d.line_id + "-color", true)
            .style("font-size", textSize);

          textContainer.append("tspan")
            .attr("x", 0)
            .attr("y", (lineCount * lineHeight) + "em")
            .text("\u00A0");

        } else {

          d.values.forEach(function(datum) {

            var label = textContainer.append("tspan")
              .attr({
                  x: 0,
                  y: (lineCount * lineHeight) + "em"
              })
              .text(datum.trueValue ? datum.trueValue : datum.value)
              .style("font-size", textSize);

            textContainer.append("tspan")
              .attr({
                  x: -label.node().getComputedTextLength(),
                  y: (lineCount * lineHeight) + "em"
              })
              .text("\u2014 ") // mdash
              .classed("mg-hover-line" + datum.line_id + "-color", true)
              .style("font-size", textSize);

            lineCount++;
          })

          textContainer.append("tspan")
            .attr("x", 0)
            .attr("y", (lineCount * lineHeight) + "em")
            .text("\u00A0");
        }
      }

      if (data.length > 0) {        

        MG.data_graphic({
          title: "Multi-line",
          data: data,
          markers: markers,
          mouseover: customRollover,
          target: "#graphDiv",
          legend: availableFileNames,
          legend_target: "#legendDiv",
          full_width: true,
          height: 600,
          interpolate: "basic",
          min_y_from_data: true,
          missing_is_hidden: true,
          brushing: true,
          brushing_interval: d3.time.second,
          utc_time: true
        })

      } else {

        notie.alert(4, "No data found", 1);
      }

    } //end if

    self.stopLoadingAnimation();
  })
}
