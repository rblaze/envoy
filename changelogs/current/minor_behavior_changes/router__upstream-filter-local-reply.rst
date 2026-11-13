Local replies sent by upstream HTTP filters via ``sendLocalReply`` now traverse the upstream HTTP
encoder filter chain and are delivered to the router, where they can be retried according to the
route retry policy without penalizing upstream host health, outlier detection, or upstream response
stats. Previously, upstream HTTP filter local replies bypassed upstream encoder filters and the
router by delegating directly to the downstream HTTP filter manager. This change can be reverted by
setting the runtime guard
``envoy.reloadable_features.upstream_filter_local_reply_via_upstream_filter_chain`` to ``false``.
