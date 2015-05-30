package com.viralgains
/**
 * Created by parampreet on 30/5/15.
 */
class QueryDTO {
    String campaignID
    String from
    List<String> groupBy
    Integer offset
    Integer limit
    String orderBy
    Map filters=[:]

    VGGranularity granularity

    Date fromTime
    Date toTime

    String timeZone="UTC"
}
