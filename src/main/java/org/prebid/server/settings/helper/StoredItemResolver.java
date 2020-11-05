package org.prebid.server.settings.helper;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.prebid.server.exception.PreBidException;
import org.prebid.server.settings.model.StoredDataType;
import org.prebid.server.settings.model.StoredItem;

import java.util.Objects;
import java.util.Set;

public class StoredItemResolver {

    private StoredItemResolver() {
    }

    /**
     * Returns {@link StoredItem} which belongs to appropriate account or throw error if not matched.
     * <p>
     * Additional processing involved because incoming prebid request may not have account defined,
     * so there are two cases:
     * <p>
     * 1. Multiple stored items were found:
     * <p>
     * - If account is not specified in prebid request - report an error.
     * <p>
     * - Otherwise, find stored item for this account or report an error if no one account matched.
     * <p>
     * 2. One stored stored item was found:
     * <p>
     * - If account is not specified in stored item or found stored item has the same account - use it.
     * <p>
     * - Otherwise, reject stored item as if there hadn't been match.
     */
    public static StoredItem resolve(StoredDataType type, String accountId, String id, Set<StoredItem> storedItems) {
        if (CollectionUtils.isEmpty(storedItems)) {
            throw new PreBidException(String.format("No stored %s found for id: %s", type, id));
        }

        final String resolvedAccountId = resolveAccountId(accountId, id);

        // at least one stored item has account
        if (storedItems.size() > 1) {
            if (StringUtils.isEmpty(resolvedAccountId)) {
                // we cannot choose stored item among multiple without account
                throw new PreBidException(String.format(
                        "Multiple stored %ss found for id: %s but no account was specified", type, id));
            }
            return storedItems.stream()
                    .filter(storedItem -> Objects.equals(resolvedAccountId, storedItem.getAccountId()))
                    .findAny()
                    .orElseThrow(() -> new PreBidException(String.format(
                            "No stored %s found among multiple id: %s for account: %s", type, id, resolvedAccountId)));
        }

        // only one stored item found
        final StoredItem storedItem = storedItems.iterator().next();
        if (StringUtils.isBlank(resolvedAccountId) || storedItem.getAccountId() == null
                || Objects.equals(resolvedAccountId, storedItem.getAccountId())) {
            return storedItem;
        }
        throw new PreBidException(
                String.format("No stored %s found for id: %s for account: %s", type, id, resolvedAccountId));
    }

    private static String resolveAccountId(String accountId, String storedItemId) {
        final String fromRequest = Objects.equals(accountId, "ACCOUNT_ID") ? null : StringUtils.stripToNull(accountId);
        if (fromRequest != null) {
            return fromRequest;
        }

        // get account id from SRID
        final String fromStoredItemId = resolveAccountIdFromStoredItemId(storedItemId);
        return isValidNumber(fromStoredItemId) ? fromStoredItemId : null;
    }

    private static String resolveAccountIdFromStoredItemId(String storedItemId) {
        return StringUtils.isNotEmpty(storedItemId)
                ? storedItemId.split("-")[0]
                : null;
    }

    private static boolean isValidNumber(String value) {
        if (StringUtils.isNumeric(value)) {
            try {
                Integer.parseInt(value);
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return false;
    }
}
