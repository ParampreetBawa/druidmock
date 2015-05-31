package com.viralgains

import com.viralgains.exception.QueryTypeNotSupportedException
import grails.converters.JSON
import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import groovy.transform.WithReadLock

import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock

/**
 * Created by parampreet on 30/5/15.
 */
class DruidService {

    def transactional = false


    def dataSource
    List<Map> cache = []

    static String DateFormatISO8601JsonFull="yyyy-MM-dd'T'HH:mm:ss.SSSX";
    static SimpleDateFormat sdf = new SimpleDateFormat(DateFormatISO8601JsonFull)
    private final ReadWriteLock lock = new ReentrantReadWriteLock()

    List doQuery(String queryStr) {
        QueryDTO queryDTO = parseQuery(queryStr)
        List<Map> response = doFilters(queryDTO)
        response = doGranularity(response,queryDTO.granularity)
        response = doGroupBy(response, queryDTO.groupBy)
        response = toDruidResponse(response)
        return response
    }

    List<Map> toDruidResponse(List<Map> response) {
        response.collect {rec->
            Long timeStr = (rec.remove('time') as String).toLong()
            Date time = new Date(timeStr)
            Map res = [timestamp:sdf.format(time)]
            Map event = [:]
            res.event = event
            event.count = rec.remove('count')
            rec.each {Object k,v->
                event[k.toString().toLowerCase()] = v
            }
            res
        }
    }

    List<Map> doGroupBy(List<Map> response, List<String> groupBy) {
        List cols = groupBy
        cols << 'time'
        Map<String, Integer> merged = [:]
        response.each { Map rec ->
            String hashKey = genKeyHash(rec, cols)
            Integer count = merged.get(hashKey) ?: 0
            merged.put(hashKey, count + 1)
        }
        response.clear()
        merged.each { key, val ->
            List<String> keys = key.split(";").toList()
            keys.remove(0)
            Map rec = [:]
            cols.eachWithIndex { col, index ->
                rec.put(col, keys.get(index))
            }
            rec.count = val
            response.add(rec)
        }
        response
    }

    String genKeyHash(Map record, List<String> cols) {
        StringBuilder str = new StringBuilder()
        cols.each {
            Object value = record.get(it)
            if(value instanceof Date)
                str.append(";" + value.getTime())
            else
                str.append(";" + value)
        }
        str
    }

    List<Map> doGranularity(List<Map> records, VGGranularity granularity) {
        Calendar cl = Calendar.getInstance()
        Date defaultTime = getBeginOfDay() - 20
        records.each {
            setTimeToGranularity(cl,it,granularity,defaultTime)
        }
        records
    }

    void setTimeToGranularity(Calendar cl, Map rec, VGGranularity granularity,Date defa) {
        Date time = rec.time
        cl.setTime(time)
        if(granularity == VGGranularity.Day) {
            cl.set(Calendar.HOUR_OF_DAY,0)
        }else if(granularity == VGGranularity.All) {
            cl.setTime(defa)
        }
        rec.time = cl.getTime()
    }

    List<Map> doFilters(QueryDTO queryDTO) {
        List<Map> response
        if(lock.readLock().tryLock(10,TimeUnit.SECONDS)) {
            response = cache.findAll { it.time >= queryDTO.fromTime && it.time <= queryDTO.toTime }.collect({new HashMap(it)})
        }
        queryDTO.filters?.each { key, val ->
            if (val instanceof List) {
                response = response.findAll({ (val as List).contains(it.get(key)) })
            } else {
                response = response.findAll { it.get(key).equals(val) }
            }
        }
        response
    }



    void buildDataCache() {
        Sql sql = new Sql(dataSource)
        List<GroovyRowResult> rs = sql.rows("select id from campaign")
        List<String> cids = rs.collect {
            it.get('id')
        }

        rs = sql.rows("select id from organization")
        List<String> pubIds = rs.collect {
            it.get('id')
        }
        if(lock.writeLock().tryLock(10,TimeUnit.SECONDS)){
            cache.clear()
            genPlayerEventData(cids, pubIds)
            lock.readLock().unlock()
        }
    }

    private void genPlayerEventData(List<String> cids, List<String> pubIds) {
        Calendar cl = Calendar.getInstance()
        List<String> actions = ["CountsAsView", "Done", "Hidden", "Load", "Played30", "Playing", "Start", "Stop", "Visible"]
        1000.times {
            Date beginOfDay = (getBeginOfDay() - (int) (Math.random() * 10)) // random day
            cl.setTime(beginOfDay)
            cl.add(Calendar.HOUR_OF_DAY, (int) (Math.random() * 24)) // random hour
            Date time = cl.getTime()
            Map event = [action: actions.get(((int) Math.random() * actions.size())),
                    time: time,
                    campaignid: cids.get(((int) Math.random() * cids.size())),
                    publisherid: pubIds.get(((int) Math.random() * pubIds.size())),
                    elapsedseconds: 5 * (int) (Math.random() * 10)  //elapsedSeconds
            ]
            cache.add(event)
        }
    }

    def parseQuery(String queryStr) {
        Map query = JSON.parse(queryStr) as Map
        if (query.queryType != 'groupBy')
            throw new QueryTypeNotSupportedException("Query Type ${query.queryType} not supported.")

        QueryDTO queryDTO = new QueryDTO()
        queryDTO.from = query.dataSource
        queryDTO.granularity = VGGranularity.valueOf(toCamelCase(query.granularity as String) as String)
        queryDTO.groupBy = query.dimensions?.collect{it.toLowerCase()}
        Map timeIntervals = parseTime(query.intervals[0])
        queryDTO.fromTime = timeIntervals.fromTime
        queryDTO.toTime = timeIntervals.toTime

        Map filter = query.filter as Map
        if(filter) {
            addFilters(filter.fields as List, queryDTO)
        }
        query.filter && query.filter['fields']
        queryDTO
    }

    void addFilters(List fields, QueryDTO queryDTO) {
        fields.each {Map field->
            if(field.type == 'and' || field.type == 'or')
                addFilters(field.fields as List,queryDTO)

            else if(field.type == 'selector'){
                String dimension = field.dimension.toLowerCase()
                Object value = queryDTO.filters.get(dimension)
                if(value == null)
                    value = []

                value << field.value
                queryDTO.filters.put(dimension,value)
            }

        }

    }

    Map parseTime(String intervals) {
        intervals.split("/").with {
            Map map = [:]
            map.fromTime = sdf.parse(it[0])
            map.toTime = sdf.parse(it[1])
            return map
        }
    }

    String toCamelCase(String gran) {
        gran.charAt(0).toUpperCase().toString() + gran.substring(1)
    }

    Date getBeginOfDay() {
        Calendar cl = Calendar.getInstance()
        cl.set(Calendar.HOUR_OF_DAY, 0)
        cl.set(Calendar.MINUTE, 0)
        cl.set(Calendar.SECOND, 0)
        cl.set(Calendar.MILLISECOND, 0)
        cl.getTime()
    }

    List<Map> getCache() {
        if(lock.readLock().tryLock(10,TimeUnit.SECONDS))
            cache
    }
}
