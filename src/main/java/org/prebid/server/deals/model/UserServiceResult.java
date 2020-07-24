package org.prebid.server.deals.model;

import lombok.AllArgsConstructor;
import lombok.Value;
import org.prebid.server.cache.model.CacheHttpCall;

@AllArgsConstructor(staticName = "of")
@Value
public class UserServiceResult {

    UserDetails userDetails;

    CacheHttpCall cacheHttpCall;

    public static UserServiceResult empty() {
        return UserServiceResult.of(UserDetails.empty(), CacheHttpCall.empty());
    }
}
