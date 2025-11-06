// Constants
var domesticAirports = [
    "FAI",
    "EWR",
    "MLI",
    "PIE",
    "MEM",
    "SJC",
    "GRR",
    "BDL",
    "CID",
    "TUS",
    "JNU",
    "IDA",
    "SAT",
    "PDX",
    "TPA",
    "BUR",
    "DAY",
    "ANC",
    "MLB",
    "ICT",
    "SMF",
    "ORD",
    "PIA",
    "KTN",
    "SNA",
    "MSY",
    "AVP",
    "SBP",
    "GEG",
    "GUM",
    "BTR",
    "STL",
    "MSN",
    "TTN",
    "PBI",
    "FAR",
    "FLL",
    "FCA",
    "EUG",
    "GSP",
    "SRQ",
    "TLH",
    "XNA",
    "RIC",
    "PHL",
    "JAX",
    "OMA",
    "LIT",
    "BOS",
    "CDV",
    "MKE",
    "SFO",
    "SDF",
    "PNS",
    "VPS",
    "MAF",
    "RSW",
    "PWM",
    "BZN",
    "ITO",
    "SIT",
    "MSP",
    "BNA",
    "SAN",
    "FAY",
    "BLI",
    "AMA",
    "GSO",
    "ACY",
    "TYS",
    "BIL",
    "SGF",
    "GTF",
    "DEN",
    "CMH",
    "ACK",
    "DAL",
    "RAP",
    "WRG",
    "TUL",
    "BWI",
    "HRL",
    "ONT",
    "ASE",
    "HPN",
    "PGD",
    "ILM",
    "BQN",
    "ROC",
    "LGB",
    "SBA",
    "OGG",
    "MSO",
    "SLC",
    "ORF",
    "LIH",
    "SUN",
    "MHT",
    "STX",
    "SHV",
    "AZA",
    "BHM",
    "PIT",
    "HSV",
    "BGR",
    "LBE",
    "EYW",
    "CLE",
    "LEX",
    "DTW",
    "CHS",
    "LGA",
    "ATW",
    "LBB",
    "SAV",
    "KOA",
    "JAN",
    "MTJ",
    "MDW",
    "FWA",
    "GPT",
    "HOU",
    "ABQ",
    "SBN",
    "MYR",
    "BFL",
    "ISP",
    "FAT",
    "CAK",
    "CMI",
    "IND",
    "MCI",
    "SYR",
    "CWA",
    "BUF",
    "EGE",
    "MIA",
    "STT",
    "AVL",
    "LAS",
    "ABE",
    "MFE",
    "LAX",
    "DCA",
    "GRB",
    "DAB",
    "ALB",
    "RDM",
    "OKC",
    "JFK",
    "JAC",
    "AUS",
    "IAH",
    "OAK",
    "HDN",
    "RNO",
    "ECP",
    "HNL",
    "PVD",
    "CHO",
    "TVC",
    "FSD",
    "IAD",
    "SJU",
    "PSC",
    "DSM",
    "CAE",
    "PSG",
    "YAK",
    "LNK",
    "SFB",
    "OME",
    "CLT",
    "BTV",
    "ELP",
    "EVV",
    "PSP",
    "BOI",
    "RDU",
    "MCO",
    "MDT",
    "LCK",
    "CHA",
    "SEA",
    "ATL",
    "CRP",
    "MFR",
    "PHX",
    "COS",
    "CVG",
];
const getMonth = {
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "May": 5,
    "Jun": 6,
    "Jul": 7,
    "Aug": 8,
    "Sep": 9,
    "Sept": 9,
    "Oct": 10,
    "Nov": 11,
    "Dec": 12
};
const getDayOfWeek = {
    "Mon": 1,
    "Tue": 2,
    "Wed": 3,
    "Thu": 4,
    "Fri": 5,
    "Sat": 6,
    "Sun": 7
};
var mainAirlineNames = [
    "Southwest",
    "Delta",
    "American",
    "United",
    "JetBlue",
    "Spirit",
    "Alaska",
    "Frontier",
    "Hawaiian"
];
var regionalAirlineNames = [
    "SkyWest",
    "Republic",
    "Envoy",
    "Endeavor",
    "PSA",
    "Allegiant",
    "Mesa",
    "Horizon"
];
var mainAirlineCodes = ["WN", "DL", "AA", "UA", "B6", "NK", "AS", "F9", "HA"];
var regionalAirlineCodes = ["OO", "YX", "MQ", "9E", "OH", "G4", "YV", "QX"];

function addDrilldown(li) {
    // Add a div to th eli called .drilldown
    // This is where we'll put the drilldown data
    var goods = li.children[0].children[1].children[0].children[1];
    if (!goods.parentElement.querySelector(".drilldown")) {
        var div = document.createElement("div");
        div.className = "drilldown";
        goods.parentElement.appendChild(div);
    }
}


async function fillDiv(div, params) {
    // The heart of the frontend, this is where we actually make the API
    // call and inject the data into the div

    // If the div is already filled, do nothing
    if (div.getAttribute("filled") == "true") return;

    // Unpack params
    var stops = params.stops;
    var dayOfWeek = params.dayOfWeek;
    var monthOfYear = params.monthOfYear;
    var airline = params.airline;
    var hour = params.hour;
    var origin = params.origin;

    // Check to see if there's a waypoint
    var waypoint;
    if (stops == 1) {
        waypoint = params.waypoint;
        layover = params.layover;
    } else {
        waypoint = "nowhere";
        layover = "nolayover";
    }

    
    origin = params.origin;
    div.style.fontSize = "18px";
    div.innerText = "Loading...";

    // Make the API call
    var api_base = 'https://romulus.tail8ddf.ts.net/'
    var API_URL = api_base +
        dayOfWeek + "/" + 
        monthOfYear + "/" + 
        origin + "/" + 
        waypoint + "/" +
        airline + "/" + 
        hour + "/" +
        layover;

    try {
        var myHeaders = new Headers();
        myHeaders.append("Content-Type", "text/plain; charset=utf8mb4;");  // Allow emojis
        
        fetch(API_URL).then(response => {
            if (!response.ok) {
                div.innerHTML = "Error. I am sorry user, I have let you down. Please<br>check the console if you want to help me debug.";
                var msg1 = "Dear user, I have let you down. I am sorry. If you want to help me debug you can email me at derek@fulton.consulting. Here are the request paramaters that caused the error: " + JSON.stringify(params);
                var msg2 = "Kindly forward these to me with the error message above if you want to have a go yourself. Thank you";
            } else {
                response.text().then(text => {
                    div.innerHTML = text;
                    
                    // Add hover functionality to the FlyOnTime data
                    var flyontimeData = div.querySelector('#flyontime-data');
                    if (flyontimeData) {
                        flyontimeData.addEventListener('mouseenter', function() {
                            var tooltip = this.querySelector('.flyontime-tooltip');
                            if (tooltip) {
                                tooltip.style.display = 'block';
                                
                                // Render Weibull distribution chart
                                var chart = tooltip.querySelector('#delay-chart');
                                if (chart && !chart.hasAttribute('data-rendered')) {
                                    // Extract shape and scale from data attributes if available
                                    var shape = parseFloat(chart.getAttribute('data-shape'));
                                    var scale = parseFloat(chart.getAttribute('data-scale'));
                                    renderWeibullChart(chart, shape, scale);
                                    chart.setAttribute('data-rendered', 'true');
                                }
                            }
                        });
                        
                        flyontimeData.addEventListener('mouseleave', function() {
                            var tooltip = this.querySelector('.flyontime-tooltip');
                            if (tooltip) {
                                tooltip.style.display = 'none';
                            }
                        });
                    }
                })
            }
        })
    } catch (error) {
        console.error('Fetch error:', error);
    }
}


function getElementByXpath(path) {
    return document.evaluate(path, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
}

function convertToMilitaryTime(timeString) {
    const date = new Date(`2000-01-01 ${timeString}`);
    const militaryTime = `${date.getHours()}${(date.getMinutes() < 10 ? '0' : '')}${date.getMinutes()}`;
    return militaryTime;
}

function ddfMutate(li) {
    // This is the function that mutates the DOM to add the FlyOnTime data
    // It's called when a user hovers over a flight

    // Start the mutation
    try {
        var goods = li.children[0].children[1].children[0].children[1].children[0];
    } catch {
        return;
    }

    // Check to make sure it's nonstop — otherwise, ignore it
    var connectionsEl = goods.children[3];
    var connections = connectionsEl.innerText;
    
    var isNonstop = connections.includes("onstop") || connections.includes("on-stop");
    var is1Stop = false;
    if (!connections.includes("onstop")) {
        // If it's not nonstop make sure it's 1 stop
        if (connections.includes("1 stop")) {
            var s1 = connections.split(" ");
            var waypoint = s1[s1.length - 1];
            var is1Stop = true;
            var s2 = connections.split('\n');
            var layover = s2[1].split(' ').slice(0, -1).join('');
            // goods.style.marginBottom = "60px";
            // First leg departure delay should be unchanged
            // Get second leg here

            // return;
        } else {
            return;
        }
    }
    
    var stuffEl = goods.children[1];
    var stuff = stuffEl.innerText.split("\n");
    var departureHour = Math.floor(stuff[0].split("–").map(convertToMilitaryTime) / 100);
    
    var airlineStr = stuff[stuff.length - 1];

    if (airlineStr.includes(",")) return;  // If it's operated by multiple airlines don't worry about it


    // If it's operated by someone else, then map it to the right airline
    if (airlineStr.includes("perated by")) {
        var airline;

        for (var i = 0; i < regionalAirlineNames.length; i++) {
            if (airlineStr.includes(regionalAirlineNames[i])) {
                var airlineCode = regionalAirlineCodes[i];
                break;
            }
        }
    } else {
        for (var i = 0; i < mainAirlineNames.length; i++) {
            if (airlineStr.includes(mainAirlineNames[i])) {
                var airlineCode = mainAirlineCodes[i];
                break;
            }
        }
    }


    // If airline is not in either main or regional airlines then don't worry about it
    if (!mainAirlineCodes.includes(airlineCode) && !regionalAirlineCodes.includes(airlineCode)) {
        return;
    }
    

    var endpointsEl = goods.children[2];
    var endpoints = endpointsEl.querySelector("span").innerText;
    var origin = endpoints.split("–")[0];

    // Remove the CO2 emissions data — nobody cares
    var co2 = goods.children[4];
    var pxEl = goods.children[5];

    if (co2.innerText.includes("kg")) {
        co2.remove();
    }

    // If it's already filled don't worry about it
    if (co2.getAttribute("filled") == "true") return;

    // Kill button + stylistic stuff for nonStop only
    if (isNonstop) {
        goods.children[goods.children.length - 1].style.flex = "0 0 calc(10%)";
        endpointsEl.style.flex = "0 0 calc(10%)";
        
        // Kill button and redefine margins after we're sure we're good to go
        var button; button = li.querySelectorAll("button")[1];
        if (button) button.parentElement.parentElement.style.display = "none";
        stuffEl.style.flex = "0 0 0"
    }
    if (is1Stop) {
        goods.children[1].style.flex = "0 0 calc(10%)";
        goods.children[1].style.marginRight = "0";
        endpointsEl.style.flex = "0 0 calc(12%)";
        connectionsEl.style.flex = "0 0 calc(12%)";
        connectionsEl.style.minWidth = "58%";
        
        // Move the price element (pxEl) to the right
        pxEl.style.flex = "0 0 auto";
        pxEl.style.float = "right";

    }


    // Get depart date
    var departDate;
    var departDateEl = getElementByXpath("/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[1]/div/div[2]/div[2]/div/div/div[1]/div/div/div[1]/div/div[1]/div/input");
    if (departDateEl) departDate = departDateEl.value;
    var returnDateEl = getElementByXpath("/html/body/c-wiz[2]/div/div[2]/c-wiz/div[1]/c-wiz/div[2]/div[1]/div/div[2]/div[2]/div/div/div[1]/div/div/div[1]/div/div[2]/div/input");
    if (returnDateEl) returnDate = returnDateEl.value;

    var outOrBackEl = getOutOrBackEl();

    if (outOrBackEl) {
        var outOrBack = outOrBackEl.innerText;
        if (outOrBack.includes("depart")) {flightDate = departDate}
        else {flightDate = returnDate}
        if (flightDate.includes(",,")) {
            flightDate = flightDate.replace(",,", ",");
        }
        var s = flightDate.split(" ");
        s = s.map(x => x.replace(',', ''));
        var dayOfWeek = getDayOfWeek[s[0]];
        monthOfYear = getMonth[s[1]];
    } else {
        // One-way
        flightDate = departDate;
        if (flightDate.includes(",,")) {
            flightDate = flightDate.replace(",,", ",");
        }
        var s = flightDate.split(" ");
        s = s.map(x => x.replace(',', ''));
        var dayOfWeek = getDayOfWeek[s[0]];
        monthOfYear = getMonth[s[1]];
    }

    // Increment counter for eventual billing (might never happen)
    sendMessage({ action: "getCounter" })
        .then(response => {
            // Eventually we make sure they haven't used all their freebies
            // but for now just let them use it
            if (response.counter <= 100) {
                // Let user use it
            } 
            else {
                // Make user pay
            }

            // Let them use it anyway during growth phase
            var params = {
                stops: 0,
                dayOfWeek: dayOfWeek,
                monthOfYear: monthOfYear,
                airline: airlineCode,
                hour: departureHour,
                origin: origin,
                waypoint: waypoint,
                layover: layover
            };
            console.log(params);
            if (isNonstop) {
                fillDiv(connectionsEl, params);
            } else {
                if (is1Stop) {
                    params.stops = 1;
                    fillDiv(connectionsEl, params);
                }
            }
        })
        .catch(error => {
            console.error(error);
        });

    li.setAttribute("hovered", "true");
    addDrilldown(li);
}


function addHovers() {
    try {
        if (!window.location.href.includes("search")) {
            return;
        }
        var x = getOutOrBackEl();
        addHeadline(x);

        var lis = getLis();

        for (var i = 0; i < lis.length; i++) {
            var li = lis[i];

            // Make the Nonstops orange with an emoji 
            var isNonstop = false;
            var spans = li.querySelectorAll("span");
            for (let span of spans) {
                if (span.innerText.includes("onstop") || span.innerText.includes("on-stop")) {
                    isNonstop = true;
                    span.style.color = "orange";
                    span.style.fontWeight = "bold";
                    span.innerText = "✈️" + " Nonstop";
                }
            }

            if (isNonstop) {
                li.onmouseover = function (){
                    if (this.getAttribute("hovered") == "true") return;
                    ddfMutate(this);
                }
            } else {
                // All flights, not just nonstop
                li.onmouseover = function (){
                    if (this.getAttribute("hovered") == "true") return;
                    ddfMutate(this);
                }

            }


        }
    } catch {
        // console.log("Failed first chunk");
    }
}

function getOutOrBackEl(){
    // Out or back?
    var h3s;
    var outOrBackEl;
    var h3s = document.querySelectorAll("h3");
    if (!h3s) return;
    for (let h3 of h3s) {
        if (h3.innerText.toLowerCase().includes("depart") && !h3.innerText.toLowerCase().includes("ther")) {
            outOrBackEl = h3;
        }
        if (h3.innerText.toLowerCase().includes("eturning")) {
            outOrBackEl = h3;
        }
    }
    return outOrBackEl;
}

function sendMessage(message) {
    return new Promise((resolve, reject) => {
      chrome.runtime.sendMessage(chrome.runtime.id, message, function(response) {
        if (chrome.runtime.lastError) {
          reject(chrome.runtime.lastError);
        } else {
          resolve(response);
        }
      });
    });
  }

function getLis() {
    // Try to get both uls if possible; no worries if we can only get one
    var ul1; ul1 = document.querySelectorAll("ul")[4];
    var ul2; ul2 = document.querySelectorAll("ul")[5];

    if (!ul1) return;  // Bail if we can't get any uls

    // Get lis from ul1
    var lis1 = ul1.querySelectorAll("li");
    
    // If ul2 found then get lis from both
    if (ul2) {
        var lis2 = ul2.querySelectorAll("li");
        var lis = [...lis1, ...lis2];
    } else {  // Otherwise just get lis from ul1
        var lis = lis1;
    }
    return lis;
}

function removeHovered(lis) {
    if (!lis) return;
    for (let li of lis) {
        li.setAttribute("hovered", "false");
    }
}


function addHeadline(outOrBackEl) {
    if (document.getElementById("license")) return;
    if (outOrBackEl) {
        tutorialEl = outOrBackEl.parentElement.querySelector("span");
        tutorialEl.innerHTML = "";
        tutorialEl.innerHTML = "<br><p id=\"headline\"> ✈️ ️️FlyOnTime — Hover over a <span style=\"color: orange\">domestic</span> flight to see your chances getting badly delayed ️️ </p>";
        tutorialEl.style.fontSize = "18px";
        tutorialEl.style.fontWeight = "bold";
        tutorialEl.style.color = "green";
        tutorialEl.style.marginTop = "10px";
    }
    Object.freeze(outOrBackEl);
}

function renderWeibullChart(svg, shape, scale) {
    // Generate modified Weibull distribution data with smoothing and fat tail
    var data = [];
    var minX = 2;  // Start slightly above 0 to avoid spike
    var maxX = 45;   // Standard 60-minute range
    var step = 0.05; // Ultra-dense data points for perfectly smooth curves
    
    // Modified Weibull with smoothing and fat tail enhancement
    for (var x = minX; x <= maxX; x += step) {
        // Base Weibull PDF: f(x; k, λ) = (k/λ) * (x/λ)^(k-1) * exp(-(x/λ)^k)
        var baseWeibull = (shape / scale) * Math.pow(x / scale, shape - 1) * Math.exp(-Math.pow(x / scale, shape));
        
        // Add very aggressive smoothing near origin to eliminate spike
        var smoothingFactor = 1;
        if (x < 8) {
            // Ultra-aggressive smoothing - start at 1% and ramp up very gradually
            smoothingFactor = 0.01 + 0.99 * Math.pow(x / 8, 3); // Cubic ramp up for ultra-smooth start
        }
        
        // Add gradual fat tail enhancement (no sharp cutoff to avoid spike)
        var fatTailFactor = 1;
        if (x > 20) {
            // Gradual enhancement starting at 20 minutes, reaching full effect at 40+ minutes
            var enhancementProgress = Math.min(1, (x - 20) / 20); // 0 to 1 over 20-40 minute range
            var tailStrength = 0.1 * enhancementProgress; // Gradual increase
            fatTailFactor = 1 + tailStrength * Math.exp(-(x - 40) / 25);
        }
        
        var density = baseWeibull * smoothingFactor * fatTailFactor;
        
        // Ensure we don't get negative or extremely large values
        density = Math.max(0, Math.min(density, 1));
        
        data.push({delay: x, density: density});
    }
    
    // Add point at x=0 with very small density to ensure smooth start
    data.unshift({delay: 0, density: data[0].density * 0.01});
    
    // Apply ULTRA-aggressive multi-pass smoothing to eliminate ANY sharp transitions
    var smoothedData = data;
    
    // First pass: larger window moving average
    var tempData = [];
    var windowSize = 7; // Much larger window
    for (var i = 0; i < smoothedData.length; i++) {
        var sum = 0;
        var count = 0;
        for (var j = Math.max(0, i - windowSize); j <= Math.min(smoothedData.length - 1, i + windowSize); j++) {
            sum += smoothedData[j].density;
            count++;
        }
        tempData.push({
            delay: smoothedData[i].delay,
            density: sum / count
        });
    }
    smoothedData = tempData;
    
    // Second pass: Gaussian-style smoothing with weighted average
    tempData = [];
    for (var i = 0; i < smoothedData.length; i++) {
        var weightedSum = 0;
        var weightSum = 0;
        for (var j = Math.max(0, i - 5); j <= Math.min(smoothedData.length - 1, i + 5); j++) {
            var weight = Math.exp(-0.5 * Math.pow((j - i) / 2, 2)); // Gaussian weight
            weightedSum += smoothedData[j].density * weight;
            weightSum += weight;
        }
        tempData.push({
            delay: smoothedData[i].delay,
            density: weightedSum / weightSum
        });
    }
    data = tempData;
    
    // D3 chart setup - same wide format
    var width = 520, height = 80;
    var margin = {top: 10, right: 15, bottom: 25, left: 15};
    var chartWidth = width - margin.left - margin.right;
    var chartHeight = height - margin.top - margin.bottom;
    
    d3.select(svg).selectAll("*").remove(); // Clear any existing content
    
    var g = d3.select(svg)
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");
    
    // Scales
    var xScale = d3.scaleLinear()
        .domain([minX, maxX])
        .range([0, chartWidth]);
        
    var maxDensity = d3.max(data, d => d.density);
    var yScale = d3.scaleLinear()
        .domain([0, maxDensity * 1.1]) // Add 10% padding
        .range([chartHeight, 0]);
    
    // Create ultra-smooth line with natural splines (mathematically smoothest)
    var line = d3.line()
        .x(d => xScale(d.delay))
        .y(d => yScale(d.density))
        .curve(d3.curveNatural);  // Natural cubic splines - mathematically optimal smoothness
    
    // Add area under curve with matching interpolation
    var area = d3.area()
        .x(d => xScale(d.delay))
        .y0(chartHeight)
        .y1(d => yScale(d.density))
        .curve(d3.curveNatural);
    
    g.append("path")
        .datum(data)
        .attr("fill", "#4285f4")
        .attr("opacity", 0.3)
        .attr("d", area);
    
    // Add frustration zone background (15+ minutes - FRUSTRATION ZONE)
    g.append("rect")
        .attr("x", xScale(15))
        .attr("y", 0)
        .attr("width", chartWidth - xScale(15))
        .attr("height", chartHeight)
        .attr("fill", "#ea4335")
        .attr("opacity", 0.15);
    
    // Draw the main curve
    g.append("path")
        .datum(data)
        .attr("fill", "none")
        .attr("stroke", "#4285f4")
        .attr("stroke-width", 2.5)
        .attr("d", line);
    
    // Add vertical line at 15 minutes (on-time threshold)
    g.append("line")
        .attr("x1", xScale(15))
        .attr("y1", 0)
        .attr("x2", xScale(15))
        .attr("y2", chartHeight)
        .attr("stroke", "#f9ab00")
        .attr("stroke-width", 1.5)
        .attr("stroke-dasharray", "3,3")
        .attr("opacity", 0.7);
    
    // Add FRUSTRATION ZONE label
    g.append("text")
        .attr("x", xScale(37.5)) // Center between 15 and 60
        .attr("y", 15)
        .attr("text-anchor", "middle")
        .attr("font-size", "11px")
        .attr("font-weight", "bold")
        .attr("fill", "#ea4335")
        .attr("opacity", 0.8)
        .text("FRUSTRATION ZONE");
    

    
    // X-axis with more ticks for wider chart
    g.append("g")
        .attr("transform", "translate(0," + chartHeight + ")")
        .call(d3.axisBottom(xScale).ticks(9).tickFormat(d => d + "m"))
        .selectAll("text")
        .style("font-size", "11px")
        .style("fill", "#ccc");
        
    g.selectAll(".domain, .tick line")
        .style("stroke", "#666");
}

function main() {

    // Wait for ul1 and main to exist because without this we can do nothing
    var ul1; var ul2; var departDate; var returnDate; var outOrBack; 
    var flightDate; var dayOfWeek; var monthOfYear; var outOrBackEl;
    while (!ul1 && !main && !ul2 && !departDate && !returnDate && 
        !outOrBack && !flightDate && !dayOfWeek && !monthOfYear && !outOrBackEl) {
        // TODO: Add timeout
        var ul1 = document.querySelectorAll("ul")[4];
        var ul2 = document.querySelectorAll("ul")[5];
        var main = document.querySelector('[role="main"]');
        // Get dates
        var outOrBackEl = getOutOrBackEl();
        if (outOrBackEl) {
            if (outOrBackEl.innerText.includes("eparting") || outOrBackEl.innerText.includes("eturning") || !outOrBackEl.innerText.includes("ther")) {
                outOrBack = outOrBackEl.innerText;
            }
        }
    }


    var mainAirlineNames = ["Southwest", "Delta", "American", "United", "JetBlue", "Spirit", "Alaska", "Frontier", "Hawaiian"];
    var regionalAirlineNames = ["SkyWest", "Republic", "Envoy", "Endeavor", "PSA", "Allegiant", "Mesa", "Horizon"];

    var mainAirlineCodes = ["WN", "DL", "AA", "UA", "B6", "NK", "AS", "F9", "HA"];
    var regionalAirlineCodes = ["OO", "YX", "MQ", "9E", "OH", "G4", "YV", "QX"];

    var getMainAirline = {};
    var getRegionalAirline = {};

    for (var i = 0; i < mainAirlineNames.length; i++) {
        getMainAirline[mainAirlineNames[i]] = mainAirlineCodes[i];
    }

    for (var i = 0; i < regionalAirlineNames.length; i++) {
    }

    // Add the hovers the instant ul1 and ul2 are established
    addHovers();

    // Check for page modification and add hovers back again
    var debounceTimer;
    var previousUrl = window.location.href;


    const observer = new MutationObserver(function(mutationsList) {
        // Iterate through the mutations
        for (let mutation of mutationsList) {
        // Perform actions based on the type of mutation
        if (mutation.type === 'childList') {
            clearTimeout(debounceTimer);
            debounceTimer = setTimeout(function() {
                removeHovered(getLis());
                addHovers();
            }, 250); 

        } else if (mutation.type === 'attributes') {
            clearTimeout(debounceTimer);
            debounceTimer = setTimeout(function() {
                removeHovered(getLis());
                addHovers();
            }, 250); 
        }
        }
    });
    
    // Configure the MutationObserver to observe the DOM subtree
    const observerConfig = { childList: true, attributes: true, subtree: true };
    observer.observe(document, observerConfig);

}

var debounceTimer;
var previousUrl = window.location.href;

const observer = new MutationObserver(function(mutationsList) {
    // Iterate through the mutations
    for (let mutation of mutationsList) {
    // Perform actions based on the type of mutation
    if (mutation.type === 'childList') {
        clearTimeout(debounceTimer);
        debounceTimer = setTimeout(function() {
            removeHovered(getLis());
            addHovers();
        }, 250); 

    } else if (mutation.type === 'attributes') {
        clearTimeout(debounceTimer);
        debounceTimer = setTimeout(function() {
            removeHovered(getLis());
            addHovers();
        }, 250); 
    }
    }
});

// Configure the MutationObserver to observe the DOM subtree
const observerConfig = { childList: true, attributes: true, subtree: true };
observer.observe(document, observerConfig);


