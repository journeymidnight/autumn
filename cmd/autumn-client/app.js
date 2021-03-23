var margin = {top: 10, right: 40, bottom: 30, left: 30},
    width = 450 - margin.left - margin.right,
    height = 400 - margin.top - margin.bottom;



    var getParams = function (url) {
      var params = {};
      var parser = document.createElement('a');
      parser.href = url;
      var query = parser.search.substring(1);
      var vars = query.split('&');
      for (var i = 0; i < vars.length; i++) {
        var pair = vars[i].split('=');
        params[pair[0]] = decodeURIComponent(pair[1]);
      }
      return params;
    };
    
jsonFileName = getParams(window.location.href).file

// append the svg object to the body of the page
var svg = d3.select("#plot")
  .append("svg")
  .attr("width", width + margin.left + margin.right)
  .attr("height", height + margin.top + margin.bottom)
  .append("g")
  .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

function maxElapsed(array) {
  n = 0
  for(let i = 0; i < array.length; i++){ 
    if (array[i].Elapsed > n) {
      n = array[i].Elapsed
    }
  }
  return n
}

console.log(jsonFileName)

d3.json(jsonFileName, function(data){
  var xScale = d3.scaleLinear()
    .domain([data[0].StartTime, data[data.length-1].StartTime])       
    .range([0, width]);  
  
  svg
  .append('g')
  .attr("transform", "translate(0," + height + ")")
  .call(d3.axisBottom(xScale));

  var yScale = d3.scaleLinear()
    //.domain([0, maxElapsed(data)])
  .domain([0, 1])
  .range([height, 0]);
  
  svg
  .append('g')
  .call(d3.axisLeft(yScale));

  svg
  .selectAll()
  .data(data)
  .enter()
  .append("circle")
  .attr("cx", function(d){ return xScale(d.StartTime)})
  .attr("cy", function(d){ return yScale(d.Elapsed) })
  .attr("r", 1)
  .attr("fill", "#9400D3")
  
});
