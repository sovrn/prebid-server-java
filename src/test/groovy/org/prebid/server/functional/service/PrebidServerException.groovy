package org.prebid.server.functional.service

class PrebidServerException extends Exception {

    final int statusCode
    final String responseBody
    final Map<String, List<String>> headers

    PrebidServerException(int statusCode, String message, Map<String, List<String>> headers) {
        this.statusCode = statusCode
        this.responseBody = message
        this.headers = headers
    }
}
