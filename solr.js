/******************************************************************
 * SOLR INTEGRATION
 *****************************************************************/

// The reserved characters in solr query strings
// Note that the "\" has to go first, as when these are substituted, that character
// will get introduced as an escape character
var solrSpecialChars = ["\\", "+", "-", "=", "&&", "||", ">", "<", "!", "(", ")", "{", "}", "[", "]", '"', "^", "~", "*", "?", ":", "/"];

// the reserved special character set with * and " removed, so that users can do quote searches and wildcards
// if they want
var solrSpecialCharsSubSet = ["\\", "+", "-", "=", "&&", "||", ">", "<", "!", "(", ")", "{", "}", "[", "]", "^", "~", "?", ":", "/"];

// values that have to be in even numbers in the query or they will be escaped
var solrPairs = ['"'];

// FIXME: esSpecialChars is not currently used for encoding, but it would be worthwhile giving the facetview an option
// to allow/disallow specific values, but that requires a much better (automated) understanding of the
// query DSL

var solr_distance_units = ["km", "mi", "miles", "in", "inch", "yd", "yards", "kilometers", "mm", "millimeters", "cm", "centimeters", "m", "meters"];

function optionsFromQuery(query) {

    function stripDistanceUnits(val) {
        for (var i=0; i < solr_distance_units.length; i=i+1) {
            var unit = solr_distance_units[i];
            if (endsWith(val, unit)) {
                return val.substring(0, val.length - unit.length);
            }
        }
        return val;
    }

    function unescapeQueryString(val) {
        function escapeRegExp(string) {
            return string.replace(/([.*+?^=!:${}()|\[\]\/\\])/g, "\\$1");
        }

        function unReplaceAll(string, find) {
            return string.replace(new RegExp("\\\\(" + escapeRegExp(find) + ")", 'g'), "$1");
        }

        // Note we use the full list of special chars
        for (var i = 0; i < solrSpecialChars.length; i++) {
            var char = solrSpecialChars[i];
            val = unReplaceAll(val, char);
        }

        return val;
    }

    var opts = {};

    // FIXME: note that fields are not supported here

    // from position
    if (query.hasOwnProperty("from")) { opts["from"] = query.from; }

    // page size
    if (query.page_size) { opts["page_size"] = query.page_size; }

    if (query["sort"]) { opts["sort"] = query["sort"]; }

    // get hold of the bool query if it is there
    // and get hold of the query string and default operator if they have been provided
    if (query.query) {
        var sq = query.query;
        var must = [];
        var qs = undefined;

        // if this is a filtered query, pull must and qs out of the filter
        // otherwise the root of the query is the query_string object
        if (sq.filtered) {
            must = sq.filtered.filter.bool.must;
            qs = sq.filtered.query;
        } else {
            qs = sq;
        }

        // go through each clause in the must and pull out the options
        if (must.length > 0) {
            opts["_active_filters"] = {};
            opts["_selected_operators"] = {};
        }
        for (var i = 0; i < must.length; i++) {
            var clause = must[i];

            // could be a term query (implies AND on this field)
            if ("term" in clause) {
                for (var field in clause.term) {
                    if (clause.term.hasOwnProperty(field)) {
                        opts["_selected_operators"][field] = "AND";
                        var value = clause.term[field];
                        if (!(field in opts["_active_filters"])) {
                            opts["_active_filters"][field] = [];
                        }
                        opts["_active_filters"][field].push(value);
                    }
                }
            }

            // could be a terms query (implies OR on this field)
            if ("terms" in clause) {
                for (var field=0; field < clause.terms.length; field=field+1) {
                    opts["_selected_operators"][field] = "OR";
                    var values = clause.terms[field];
                    if (!(field in opts["_active_filters"])) {
                        opts["_active_filters"][field] = [];
                    }
                    opts["_active_filters"][field] = opts["_active_filters"][field].concat(values);
                }
            }

            // could be a range query (which may in turn be a range or a date histogram facet)
            if ("range" in clause) {
                // get the field that we're ranging on
                var r = clause.range;
                var fields = Object.keys(r);
                var field = false;
                if (fields.length > 0) {
                    field = fields[0];
                }

                if (field) {
                    var rparams = r[field];
                    var range = {};
                    if ("lt" in rparams) { range["to"] = rparams.lt; }
                    if ("gte" in rparams) { range["from"] = rparams.gte; }
                    opts["_active_filters"][field] = range;
                }
            }

            // cound be a geo distance query
            if ("geo_distance_range" in clause) {
                var gdr = clause.geo_distance_range;

                // the range is defined at the root of the range filter
                var range = {};
                if ("lt" in gdr) { range["to"] = stripDistanceUnits(gdr.lt); }
                if ("gte" in gdr) { range["from"] = stripDistanceUnits(gdr.gte); }

                // FIXME: at some point we may need to make this smarter, if we start including other data
                // in the geo_distance_range filter definition
                // then we have to go looking for the field name
                for (var field=0; field < gdr.length; field=field+1) {
                    if (field === "lt" || field === "gte") { continue; }
                    opts["_active_filters"][field] = range;
                    break;
                }
            }

            // FIXME: support for statistical facet and terms_stats facet
        }

        if (qs) {
            if (qs.query_string) {
                var string = unescapeQueryString(qs.query_string.query);
                var field = qs.query_string.default_field;
                var op = qs.query_string.default_operator;
                if (string) { opts["q"] = string; }
                if (field) { opts["searchfield"] = field; }
                if (op) { opts["default_operator"] = op; }
            } else if (qs.match_all) {
                opts["q"] = "";
            }
        }

        return opts;
    }
}

function getFilters(params) {
    var options = params.options;

    // function to get the right facet from the options, based on the name
    function selectFacet(name) {
        for (var i = 0; i < options.facets.length; i++) {
            var item = options.facets[i];
            if ('field' in item) {
                if (item['field'] === name) {
                    return item;
                }
            }
        }
    }

    function termsFilter(facet, filter_list) {
        if (facet.logic === "AND") {
            var filters = [];
            for (var i=0; i < filter_list.length; i=i+1) {
                var value = filter_list[i];
                var tq = {"term" : {}};
                tq["term"][facet.field] = value;
                filters.push(tq);
            }
            return filters;
        } else if (facet.logic === "OR") {
            var tq = {"terms" : {}};
            tq["terms"][facet.field] = filter_list;
            return [tq];
        }
    }

    function rangeFilter(facet, value) {
        var rq = {"range" : {}};
        rq["range"][facet.field] = {};
        if (value.to) { rq["range"][facet.field]["lt"] = value.to; }
        if (value.from) { rq["range"][facet.field]["gte"] = value.from; }
        return rq;
    }

    function geoFilter(facet, value) {
        var gq = {"geo_distance_range" : {}};
        if (value.to) { gq["geo_distance_range"]["lt"] = value.to + facet.unit; }
        if (value.from) { gq["geo_distance_range"]["gte"] = value.from + facet.unit; }
        gq["geo_distance_range"][facet.field] = [facet.lon, facet.lat]; // note the order of lon/lat to comply with GeoJSON
        return gq;
    }

    function dateHistogramFilter(facet, value) {
        var rq = {"range" : {}};
        rq["range"][facet.field] = {};
        if (value.to) { rq["range"][facet.field]["lt"] = value.to; }
        if (value.from) { rq["range"][facet.field]["gte"] = value.from; }
        return rq;
    }

    // function to make the relevant filters from the filter definition
    function makeFilters(filter_definition) {
        var filters = [];
        for (var field in filter_definition) {
            if (filter_definition.hasOwnProperty(field)) {
                var facet = selectFacet(field);

                // FIXME: is this the right behaviour?
                // ignore any filters from disabled facets
                if (facet.disabled) { continue; }

                var filter_list = filter_definition[field];

                if (facet.type === "terms") {
                    filters = filters.concat(termsFilter(facet, filter_list)); // Note this is a concat not a push, unlike the others
                } else if (facet.type === "range") {
                    filters.push(rangeFilter(facet, filter_list));
                } else if (facet.type === "geo_distance") {
                    filters.push(geoFilter(facet, filter_list));
                } else if (facet.type == "date_histogram") {
                    filters.push(dateHistogramFilter(facet, filter_list));
                }
            }
        }
        return filters;
    }

    // read any filters out of the options and create an array of "must" queries which
    // will constrain the search results
    var filter_must = [];
    if (options.active_filters) {
        filter_must = filter_must.concat(makeFilters(options.active_filters));
    }
    if (options.predefined_filters) {
        filter_must = filter_must.concat(makeFilters(options.predefined_filters));
    }
    if (options.fixed_filters) {
        filter_must = filter_must.concat(options.fixed_filters);
    }

    return filter_must;
}

function solrQuery(params) {
    // break open the parameters
    var options = params.options;
    var include_facets = "include_facets" in params ? params.include_facets : true;
    var include_fields = "include_fields" in params ? params.include_fields : true;

    var filter_must = getFilters({"options" : options});

    // search string and search field produce a query_string query element
    var querystring = options.q;
    var searchfield = options.searchfield;
    var default_operator = options.default_operator;
    var ftq = undefined;
    if (querystring) {
        ftq = {'query_string' : { 'query': fuzzify(querystring, options.default_freetext_fuzzify) }};
        if (searchfield) {
            ftq.query_string["default_field"] = searchfield;
        }
        if (default_operator) {
            ftq.query_string["default_operator"] = default_operator;
        }
    } else {
        ftq = {"match_all" : {}};
    }

    // if there are filter constraints (filter_must) then we create a filtered query,
    // otherwise make a normal query
    var qs = undefined;
    if (filter_must.length > 0) {
        qs = {"query" : {"filtered" : {"filter" : {"bool" : {"must" : filter_must}}}}};
        qs.query.filtered["query"] = ftq;
    } else {
        qs = {"query" : ftq};
    }

    // sort order and direction
    if (options.sort && options.sort.length > 0) {qs['sort'] = options.sort;} else {qs['sort'] = "";};

    qs['page_size'] = options.page_size ? options.page_size : 100;

    // fields and partial fields
    if (include_fields) {
        qs['fields'] = options.fields ? options.fields : "";
        qs['partial_fields'] = options.partial_fields ? options.partial_fields : "";
        qs["script_fields"] = options.script_fields ? options.script_fields : "";
    }

    // paging (number of results, and start cursor)
    if (options.from !== undefined) {
        qs["from"] = options.from;
    }

    qs["query_parameter"] = options.query_parameter ? options.query_parameter : "q";

    // facets
    if (include_facets) {
        qs['facets'] = {};
        for (var item = 0; item < options.facets.length; item++) {
            var defn = options.facets[item];
            if (defn.disabled) { continue; }

            var size = defn.size;

            // add a bunch of extra values to the facets to deal with the shard count issue
            size += options.solr_facet_inflation;

            var facet = {};
            if (defn.type === "terms") {
                facet["terms"] = {"field" : defn["field"], "size" : size, "order" : defn["order"]};
            } else if (defn.type === "range") {
                var ranges = [];
                for (var r=0; r < defn["range"].length; r=r+1) {
                    var def = defn["range"][r];
                    var robj = {};
                    if (def.to) { robj["to"] = def.to; }
                    if (def.from) { robj["from"] = def.from; }
                    ranges.push(robj);
                }
                facet["range"] = { };
                facet["range"][defn.field] = ranges;
            } else if (defn.type === "geo_distance") {
                facet["geo_distance"] = {};
                facet["geo_distance"][defn["field"]] = [defn.lon, defn.lat]; // note that the order is lon/lat because of GeoJSON
                facet["geo_distance"]["unit"] = defn.unit;
                var ranges = [];
                for (var r=0; r < defn["distance"].length; r=r+1) {
                    var def = defn["distance"][r];
                    var robj = {};
                    if (def.to) { robj["to"] = def.to; }
                    if (def.from) { robj["from"] = def.from; }
                    ranges.push(robj);
                }
                facet["geo_distance"]["ranges"] = ranges;
            } else if (defn.type === "statistical") {
                facet["statistical"] = {"field" : defn["field"]};
            } else if (defn.type === "terms_stats") {
                facet["terms_stats"] = {key_field : defn["field"], value_field: defn["value_field"], size : size, order : defn["order"]};
            } else if (defn.type === "date_histogram") {
                facet["date_histogram"] = {field : defn["field"], interval : defn["interval"]};
            }
            qs["facets"][defn["field"]] = facet;
        }

        // and any extra facets
        // NOTE: this does not include any treatment of the facet size inflation that may be required
        if (options.extra_facets) {
            $.extend(true, qs['facets'], options.extra_facets );
        }
    }

    return qs;
}

function fuzzify(querystr, default_freetext_fuzzify) {
    var rqs = querystr;
    if (default_freetext_fuzzify !== undefined) {
        if (default_freetext_fuzzify == "*" || default_freetext_fuzzify == "~") {
            if (querystr.indexOf('*') === -1 && querystr.indexOf('~') === -1 && querystr.indexOf(':') === -1) {
                var optparts = querystr.split(' ');
                pq = "";
                for ( var oi = 0; oi < optparts.length; oi++ ) {
                    var oip = optparts[oi];
                    if ( oip.length > 0 ) {
                        oip = oip + default_freetext_fuzzify;
                        oip = default_freetext_fuzzify == "*" ? "*" + oip : false;
                        pq += oip + " ";
                    }
                }
                rqs = pq;
            }
        }
    }
    return rqs;
}

function serialiseQueryObject(queryobj) {
    // set default URL params
    var urlparams = "wt=json&";
    for (var item in queryobj.default_url_params) {
        urlparams += item + "=" + queryobj.default_url_params[item] + "&";
    }
    // do paging params
    var pageparams = "";
    for (var item in queryobj.paging) {
        pageparams += queryobj.solr_paging_params[item] + "=" + queryobj.paging[item] + "&";
    }
    var rows = queryobj.page_size;
    pageparams += "rows=" + rows + "&";
    var start = queryobj.from ? queryobj.from : 0;
    pageparams += "start=" + start + "&";
    // set facet params
    var urlfilters = "";
    for (var item in queryobj.facets) {
        urlfilters += "facet.field=" + queryobj.facets[item]['field'] + "&";
        if ( queryobj.facets[item]['size'] ) {
            urlfilters += "f." + queryobj.facets[item]['field'] + ".facet.limit=" + queryobj.facets[item]['size'] + "&";
        }
    }
    if (queryobj.sort) {
        urlfilters += "sort=";
        for (var sorter in queryobj.sort) {
            if (queryobj.sort.hasOwnProperty(sorter)) {
                var keyname = Object.keys(queryobj.sort[sorter])[0];
                urlfilters += keyname + "+" + queryobj.sort[sorter][keyname]["order"]+",";
            }
        }
        urlfilters = urlfilters.substr(0, urlfilters.length - 1) + "&";
    }
    if (queryobj.facets && queryobj.facets.length > 0 ) {
        urlfilters += "facet=on&";
    }
    // build starting URL
    var theurl = urlparams + pageparams + urlfilters;
    // add default query values
    // build the query, starting with default values
    var query = "";
    //for (var item in options.predefined_filters) {
    // query += item + ":" + options.predefined_filters[item] + " AND ";
    //}
    $('.facetview_filterselected', queryobj.facets).each(function() {
        query += $(this).attr('rel') + ':"' +
        $(this).attr('href') + '" AND ';
    });
    // add any freetext filter
    if (queryobj.q != "") {
        query += queryobj.q;
    }
    if (!query.endsWith('*')) {
        query += '*';
    }
    query = query.replace(/ AND $/,"");
    // set a default for blank search
    if (query == "" ||  !queryobj.q) {
        query = "*:*";
    }
    theurl += queryobj.query_parameter + '=' + query;
    return theurl;
}

// closure for elastic search success, which ultimately calls
// the user's callback
function solrSuccess(callback) {
    return function(data) {
        var resultobj = {
            "records" : data.response.docs,
            "start" : data.response.start,
            "found" : data.response.numFound,
            "facets" : {}
        };

        if (data.facet_counts) {
            for (var item in data.facet_counts.facet_fields) {
                var facetsobj = new Object();
                var count = 0;
                for ( var each in data.facet_counts.facet_fields[item]) {
                    if ( count % 2 == 0 ) {
                        facetsobj[ data.facet_counts.facet_fields[item][each] ] = data.facet_counts.facet_fields[item][count + 1];
                    }
                    count += 1;
                }
                resultobj["facets"][item] = facetsobj;
            }
        }

        // load the results into the records part of the result object
        for (var item = 0; item < data.response.numFound; item++) {
            var res = data.response.docs[item];
            if ("fields" in res) {
                // partial_fields and script_fields are also included here - no special treatment
                resultobj.records.push(res.fields);
            } else {
                resultobj.records.push(res);
            }
        }

        callback(data, resultobj);
    };
}

function doSolrQuery(params) {
    // extract the parameters of the request
    var success_callback = params.success;
    var complete_callback = params.complete;
    var querystring = serialiseQueryObject(params.queryobj);
    var search_url = params.search_url + querystring;
    var datatype = params.datatype;

    // make the call to the solr web service
    $.ajax({
        type: "get",
        url: search_url,
        processData: false,
        dataType: datatype,
        //dataType: "text",
        //contentType: "text/plain",
        //crossDomain: true,
        //converters: "text json",
        jsonp: "json.wrf",
        success: solrSuccess(success_callback),
        complete: complete_callback,
        error: function(jqXHR, textStatus, errorThrown){debugger;}
    });
}
