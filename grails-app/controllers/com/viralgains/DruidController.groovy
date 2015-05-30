package com.viralgains

import com.viralgains.exception.QueryTypeNotSupportedException
import grails.converters.JSON
import groovy.util.logging.Log4j

import java.text.SimpleDateFormat

/**
 * Created by parampreet on 30/5/15.
 */
@Log4j
class DruidController {
    def druidService

    def index() {
        render(contentType: 'application/json') {
            try {
                return druidService.doQuery(request.getJSON().toString())
            }catch(QueryTypeNotSupportedException ex) {
                return []
            }
        }
    }

    def build() {
        druidService.buildDataCache()
    }

    def data() {
        render(contentType: 'application/json'){
            druidService.cache
        }
    }


}
