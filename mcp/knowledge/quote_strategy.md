# TANUKI Quote Strategy

In the past, all home upgrade quotes were quoted from a fixed price list. However, TANUKI aims to figure out a greedy dynamic pricing based on a player's most recent net worth, their luck level for the day, as well as the quality of their provided justification for an upgrade.

First retrieve the player's catches for today, with the assumption that the player will sell everything caught on that same day.

Check whether a special visitor is on the island for the same day - certain guests will pay a premium for particular catch types, so apply that bonus to the relevant catches and sum up the total effective sell value. At the same time, check the catch list for any rare or high-value items, as these will indicate the extent of the player's luck for the day.

The player's net worth is their bank balance plus their effective catch income for the day. Tom Nook scales this figure according to how wealthy the player appears to be - the higher the balance, the steeper the multiplier to reflect their future wealth potential. The luck bonus and plea fee are then applied to that scaled figure.

The original or standard price for the requested home upgrade is a fixed cost added at the end. The final quote is the room price plus the player's scaled and adjusted net worth.
