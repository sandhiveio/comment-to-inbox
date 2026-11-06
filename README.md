# comment-to-inbox

A backend prototype for one Instagram flow. A person comments with an allowlisted phrase such as `test`. The account sends a private reply asking for an email. After that DM is confirmed, the system queues a public reply and waits for a message. The first accepted valid address is stored as a lead, and the system sends the link in a DM.

Three ways to run it:

- **Live test.** Uses the data in `config/local.json`. That file can be shared separately. It is not in Git, because it holds keys and account identifiers.
- **Dry run.** Reads real comments through Zernio and only parses them, with no public replies and no sent messages. In the check, `teSt` started the flow and `What I need to do?` went to an operator. Real outbound sends were outside that check.
- **Test on fake data.** The `demo` command and the automated tests use `FakeInstagram`, with no real API calls and no keys.

## Take-home assumptions

- **One project, one account, one config, and one container.** This is the simple, readable case: every setting lives in one config, so it is easy to see and to change on restart. Neither one file per account nor one container per account would work well for hundreds of customers. The assignment did not require that scale.
- **Docker.** The project is packaged in Docker on purpose, so it is easy to move and so isolated projects can run in parallel.
- **Zernio API.** It can be set up quickly, and for the demo it has no awkward limits. On a real job the first priority would be the official Meta API plus at least one backup layer, such as Zernio.
- **Polling the API instead of webhooks.** Polling fits a light, portable project and is easier to operate for a demonstration. It is weaker on reply speed and is easy to overwhelm with requests once there are many posts and conversations.
- **The first accepted event wins.** This does not conflict with the assignment. In my view this order solves the problem as well as the alternatives: the flow continues immediately, without waiting to reorder deliveries.
- **One worker thread per account.** Each live account then replies within a predictable time, cannot sit forever at the tail of someone else's queue, and cannot send too many requests at once when something fails. That lowers the risk of an unbounded wait and of spam enforcement.
- **Private reply first, then the public reply.** If the private reply succeeded, it is worth saying so in public: by the time the person sees the comment, the DM has already been delivered. The reverse order can leave a misleading comment when the DM could not be delivered. A confirmed permanent failure can still use a separately configured public reply.
- **Several handwritten variants.** Both public replies and messages can be chosen from several texts written by hand, so the dialogue is more varied. The chosen text is stored with the action and reused on retry.
- **Escalation to an operator.** When a signal is not obvious, the system can send the case to an operator through an aggregator. This is a personal assumption, so disputed cases and unclear signals can still be handled. The prototype queues that notice for the test adapter. In production an aggregator such as Knock can deliver the same event to the team's channel.
- **Stop words.** A stop-word list ends communication so the presence of automation does not spoil the impression. The command cancels queued sends and lead exports.
- **One transaction for the lead.** Accepting the email, saving the lead, and queueing the link happen in one SQLite transaction. External lead export runs separately.
- **Optional fuzzy keyword match.** Added for cases where typos are common and would otherwise lose leads. A near miss is handed to an operator instead of being dropped silently. The option is off by default.
- **Daily statistics.** The system collects and stores incremental counters for each day and can export them for review. That is needed to check stability.
- **A bounded set of posts.** `pricing` and the other allowlist phrases apply only to posts that are explicitly enabled. There is no mode for every post, including future ones. The assignment can be read either way, and this is a deliberate limit on scope: posts are set explicitly, and `sync-posts` adds new ones. A search across the account's whole history would be the same pass with deduplication. The structure would barely change. What would still need design is when a post goes stale, a webhook for current posts, and a rule for which of them to monitor: words in the caption, or a model score.

### Running the three modes

~~~bash
docker compose build
~~~

Live test. This needs `config/local.json`, which is not in Git. The container reads comments and sends messages on its own.

~~~bash
docker compose --profile local up -d --build local
docker compose exec -T local python -m comment_to_inbox status
docker compose --profile local stop local
~~~

Dry run. The same local config, read and parse only, with no sends.

~~~bash
docker compose --profile local run --rm local python -m comment_to_inbox dry-run
~~~

Test on fake data. `demo` walks the flow on `FakeInstagram`. `test` runs the automated tests.

~~~bash
docker compose run --rm app python -m comment_to_inbox demo
docker compose --profile test run --rm test
~~~

### Configuration

The sample is `config/project.json`. The local config `config/local.json` is excluded from Git and from the Docker image. Keep API keys and personal identifiers in the local config.

| Field | Role |
| --- | --- |
| `account_id`, `account_username` | The connected account, and the name used to find it and to skip its own comments |
| `comment_allowlist` | Phrases that start the flow |
| `blocklist` | Opt-out commands |
| `escalation_list` | Phrases that hand the case to an operator |
| `keyword_typo_escalation` | Hand off on a keyword typo. Off by default |
| `posts` | Posts with `id`, `url`, `enabled`, and `enabled_at` |
| `email_wait_seconds` | How long to wait for an email after the first DM is confirmed. Default is seven days |
| `public_reply_variants` | Public replies after the DM is confirmed |
| `public_fallback_variants` | Optional public replies after a permanent failure of the first DM |
| `email_request_variants`, `email_retry_variants` | Variants for the first email ask and for re-asks |
| `link_url`, `link_dm_variants` | The link and the DM variants that carry it. Each variant must contain the link |
| `notifications.workflow`, `notifications.api_key` | Notification workflow and its key |
| `zernio_base_url`, `zernio_api_key` | Zernio Inbox API. `dry-run` only reads. `serve --adapter zernio` also sends |
| `rate_limit_per_minute`, `send_timeout_seconds`, `retry_delay_seconds`, `action_deadline_seconds` | Send budget, timeout, retry delay, and action deadline |

Older fields `keyword`, `keywords`, and the single templates `public_reply_text`, `email_request_text`, `email_retry_text`, `link_dm_text` are still accepted. A single string is read as a one-element list. `blocklist` and `escalation_list` are required and may be empty.

```json
{
  "account_id": "ig-demo-alpha",
  "account_username": "demo",
  "comment_allowlist": ["pricing", "test"],
  "keyword_typo_escalation": false,
  "blocklist": ["stop", "unsubscribe", "do not message me", "leave me alone"],
  "escalation_list": ["what do i need to do", "what i need to do", "did not receive", "didn't receive", "didn't get it", "nothing arrived", "operator", "help", "spam"],
  "posts": [
    {"id": "post-alpha-1", "url": "https://www.instagram.com/p/post-alpha-1/", "enabled": true},
    {"id": "post-alpha-2", "url": "https://www.instagram.com/p/post-alpha-2/", "enabled": true}
  ],
  "notifications": {"workflow": "operator-handoff", "api_key": ""},
  "zernio_base_url": "https://zernio.com/api/v1",
  "zernio_api_key": "",
  "public_reply_variants": ["Thanks for commenting. I sent you a message."],
  "public_fallback_variants": ["We couldn't message you. If you'd still like the details, please send us a DM."],
  "email_wait_seconds": 604800,
  "email_request_variants": ["Reply with your email and I will send the link."],
  "email_retry_variants": ["I still need an email address in your reply."],
  "link_url": "https://example.com/alpha-guide",
  "link_dm_variants": ["Here is the link: https://example.com/alpha-guide"],
  "rate_limit_per_minute": 30,
  "send_timeout_seconds": 5,
  "retry_delay_seconds": 2,
  "action_deadline_seconds": 604800
}
```

## Deployment invariant

One active container runs for each `account_id`. Compose does not check this. If two containers serve one account with different volumes, each applies its own budget and its own deduplication. The send lock is inside the process.

## How the three modes behave

The build creates the image `comment-to-inbox:local`.

`demo` writes to the `app-data` volume and the account `ig-demo-alpha`. Running it again prints the stored rows and does not start a second run. The `project-b` profile is a second isolated project: `config/project-b.json`, the `project-b-data` volume, and the account `ig-demo-beta`. It can run beside the first one. `demo` starts its own worker, so it must not run on the same volume at the same time as `serve`.

The live test is one container, `config/local.json`, and the `local-data` volume. The file is not in Git and not in the image. The process exits on startup if `zernio_base_url`, `zernio_api_key`, or `account_id` is missing. `account_id` is the Zernio account id. `send_timeout_seconds` must cover one call: a timeout is recorded as `unknown`, and the private reply is not sent again. About every 20 seconds the service reads comments on enabled posts and incoming direct messages. The comment list is cached for up to 10 minutes, so polling is only for this demo. In production the event source is webhooks, for example `comment.received` and `message.received`. From a second account that has not written to this one yet, leave `test` on an enabled post. The container sends the private email request, then the public confirmation, and turns an address in that same thread into the link. `status` shows the run. An incoming DM must be strictly later than `boundary`.

## Architecture and storage

The integration passes a normalized event with a stable `external_id`, a person id, and the platform time `occurred_at`. A comment carries `post_id`, `comment_id`, and text. A DM carries text. The integration filters out the account's own outbound messages. Events for another account are rejected.

Accepting an event and queueing an action happen in a short transaction. The worker claims a ready action, checks the deadline, opt-out, and budget, then calls the adapter outside the transaction with a timeout. The next short transaction records the result, moves the run to the new state, and creates the next task when needed. While the adapter waits for the HTTP response, event intake continues.

SQLite uses `WAL`, `busy_timeout=5000`, `foreign_keys=ON`, and `synchronous=NORMAL`. The schema is in `comment_to_inbox/db.py`.

| Table | Contents and uniqueness |
| --- | --- |
| `inbound_events` | Inbound events and the classification result. Unique by account and external id |
| `runs` | One run per account and person. The source comment is unique too |
| `outbound_actions` | Outbound actions. Unique by run, kind, and slot |
| `action_results`, `budget_spend` | Results and budget spend. One row of each kind per attempt |
| `opt_outs` | Opt-outs by account and person |
| `leads`, `lead_exports` | One lead per run and one export task per lead |
| `operator_handoffs`, `operator_notifications` | One active handoff per person and one delivery task per handoff |
| `daily_counts` | Counters by UTC day, account, and event |

The first email request uses slot `0`. Re-asks use slots `1`–`3`. The chosen action text is stored in `outbound_actions.body`. A retry uses that same text after a restart and after the config is edited. The lead UUID is the stable export key. Opening an old volume creates missing lead and export tables. Addresses from old runs are not exported automatically. `status` shows whether an address is present with `email=yes` or `email=no`.

## Post management

The assignment does not say whether to reply under any post on the account, including future ones, or only under a chosen set. This prototype does the second. `pricing` and the other allowlisted phrases match only on posts that are explicitly enabled. There is no mode for every post, including future ones. That is an acceptable limit on scope. The operator sets a finite list, and `sync-posts` adds new rows as disabled. A search over the account's whole history would be the same pass with deduplication. The extra design would be when a post goes stale, a webhook subscription for current posts, and a filter for which captions to monitor, by keywords or by a model score. A bounded list lowers the chance of replies under unrelated posts. In production the inbound stream is the account webhook. Events from posts outside the list are stored and do not start the flow. Polling the comment list is kept only for this light test app: fewer enabled posts means fewer requests and a shorter sweep.

`sync-posts` reads publications with pagination and skips known ids. By default it shows a preview. `--apply` atomically adds new rows with `enabled: false`. The operator enables the posts they want and sets `enabled_at`. Comments before that moment are ignored. Post discovery and dry run do not queue sends.

~~~bash
python -m comment_to_inbox sync-posts
python -m comment_to_inbox sync-posts --apply
~~~

In the demo the key is empty. Tests use a test publication source.

## Routing and states

After deduplication, rules apply in this order:

1. **Opt-out.** The command must be the whole message after normalization and after trailing punctuation is removed. Examples: `STOP!`, `UNSUBSCRIBE`, `do not message me`, `leave me alone`. The system stores the opt-out, closes the active run, and cancels pending sends and exports. The opt-out is checked again before the external call. A call already in flight may finish. An exported lead is deleted in the external system separately.
2. **Operator handoff.** Phrases are matched case-insensitively as whole words or word sequences. Text inside a URL or an email is excluded. `What I need to do`, `test — WHAT I NEED TO DO next?`, and `Is this spam?` open a handoff with no automatic reply. The active run is closed and pending sends are cancelled. A handoff is possible without a run.
3. **Allowlisted comment.** `test` and `TEST!` start the flow. `contest` is skipped. The first eligible comment stored by the system wins. Webhooks are not held to be reordered. Later comments from the person, including on another post or after the wait expires, do not start a new run.
4. **Ordinary DM.** The first syntactically valid address is accepted and lowercased. Among already stored eligible DMs, the earliest one with an address is taken. In a race, the transaction that first closes the wait wins. A message without an address consumes one re-ask. A webhook duplicate and a repeated external send do not consume that attempt. After three re-asks, the next reply without an address moves the run to `exhausted` and opens an operator handoff.

When `keyword_typo_escalation` is on, a word one insertion, deletion, substitution, or adjacent transposition away from a keyword of four or more characters is handed to an operator. For example, `guid` when the keyword is `guide`. An exact match keeps the normal behavior. `stop@example.com` can be accepted as an address.

~~~mermaid
stateDiagram-v2
    [*] --> email_request_pending: eligible comment accepted
    email_request_pending --> awaiting_email: first DM confirmed
    email_request_pending --> private_reply_failed: permanent failure
    email_request_pending --> private_reply_unknown: outcome unknown
    email_request_pending --> link_dm_pending: an eligible DM is already stored
    awaiting_email --> link_dm_pending: valid email accepted
    awaiting_email --> email_request_pending: re-ask needed
    awaiting_email --> exhausted: re-asks exhausted
    awaiting_email --> expired: wait elapsed
    awaiting_email --> handed_off: operator handoff
    awaiting_email --> stopped: opt-out
    link_dm_pending --> completed: link send confirmed
~~~

Confirming the first DM opens the email wait and queues the public reply. A public-reply failure does not interrupt the wait. A permanent failure of the first DM opens a handoff and may queue the configured public fallback when there is no opt-out and no earlier handoff. An unknown outcome of the first DM opens a handoff without a fallback. An unknown outcome of the link send does not create the next action.

The seven-day wait is counted from the confirmed first DM. Re-asks do not extend it. After it expires, a late email does not create a lead. Instagram's send window is checked separately before the call. In the prototype the first private reply and the public reply use a seven-day bound from the comment. A re-ask and the link use 24 hours from the person's latest DM.

### How a DM is tied to a request

A DM that arrives while the email request is being sent is stored as `parked`. After confirmation the adapter returns `sent_at`. The system compares it with the inbound message `occurred_at`. An equal or earlier time means an old message. A later message is processed in the transaction that opens the wait. On a re-ask, messages between the previous boundary and the new one belong to the previous request. If one of them contains an email, it closes the wait. Extra messages without an address in that gap do not create another re-ask.

## Limits, failures, and recovery

One worker holds at most one outbound call at a time. If a request takes `R` seconds, the upper bound for the account is `1/R` sends per second. A slow call delays this account's queue. Other accounts keep working in their own containers.

Each attempt reserves a slot in `budget_spend`, including retries and attempts with an unknown result. When the 60-second sliding window is full, the action is deferred until a slot frees. An overdue action finishes without calling the adapter.

| Outcome | Action |
| --- | --- |
| `success` | Store the message id and time, update the run, and queue the next step |
| `explicit_failure` | If it is known that there was no effect and the error can be retried, defer the same task until `not_before` |
| `unknown` | Store the uncertain outcome and stop automatic retries |

A timeout, a lost response, HTTP 5xx, and an unfinished `sending` found at startup count as an unknown outcome. The budget for that attempt is kept.

### Adapters

The send interface is `send(OutboundRequest) -> SendResult`. `OfficialInstagramAdapter` is the primary option. `ZernioAdapter` is the reserve. Switching is allowed until the first provider has accepted the request. `serve` and `demo` use `FakeInstagram`.

| Channel | Official API | Zernio |
| --- | --- | --- |
| `comment_reply` | `POST /{comment-id}/replies` | Comment reply through Inbox |
| `private_reply` | `POST /{ig-user-id}/messages` with `recipient.comment_id` | `POST /v1/inbox/comments/{postId}/{commentId}/private-reply` |
| `direct_message` | `POST /{ig-user-id}/messages` with `recipient.id` | Send into the thread through Inbox |

Instagram Login uses `graph.instagram.com`. A Facebook Page token uses `graph.facebook.com`. Typical permissions: `instagram_business_basic`, `instagram_business_manage_comments`, `instagram_business_manage_messages`. A private reply by itself does not open the standard messaging window. On Zernio a confirmed `messageId` means success, `privateReplyConsumed: true` rules out a safe retry, and `privateReplyOutcome: "unknown"` means the outcome is uncertain.

## Lead export

The email-accept transaction saves the lead, updates the run, and queues the export and the link DM on independent queues. The Instagram worker does not wait for the external store.

~~~bash
docker compose exec -T app python -m comment_to_inbox export-leads --once
~~~

The command processes a small batch and exits. `FakeLeadExporter` upserts by the lead UUID. `CTI_EXPORT_RESULT=temporary` and `CTI_EXPORT_RESULT=permanent` reproduce errors. For scheduled export, call the command from cron on the host. The service must be running.

~~~cron
* * * * * cd /opt/comment-to-inbox && docker compose exec -T app python -m comment_to_inbox export-leads --once
* * * * * cd /opt/comment-to-inbox && docker compose --profile project-b exec -T project-b python -m comment_to_inbox export-leads --once
~~~

A claim stores `claim_expires_at` and `claim_token`. The external upsert runs with a timeout after commit. The result is written only with the current token. After a failure the row is available again when the claim expires, and the retry uses the same UUID. A temporary error updates `attempt_count` and `next_attempt_at`. A permanent one shows in `status` as `permanent_failure`. Export logs contain the id and the exception type, without the address.

## Operator notifications

The handoff and the queue row are saved in one transaction. Reasons: `escalation`, `email_exhausted`, `private_reply_failed`, `private_reply_unknown`. The notice contains the project, the reason, and a UUID for finding the thread. One active handoff creates one notice.

~~~bash
docker compose exec -T app python -m comment_to_inbox notify-operators --once
docker compose exec -T app python -m comment_to_inbox clear-handoff --person-id live-reader
~~~

`FakeOperatorNotifier` prints the event to the CLI. `CTI_NOTIFY_RESULT=temporary` reproduces a failure. A failed delivery can be retried. A successful delivery does not by itself close the handoff. The operator continues the conversation by hand. `clear-handoff` closes the handoff without restoring cancelled actions, restarting the flow, or removing an opt-out. A future `KnockOperatorNotifier` can start the `operator-handoff` workflow with the same fields. The delivery channel is configured in Knock.

## Daily statistics

`daily_counts` is updated in the event transaction. A confirmed send is counted once, when the action moves to `succeeded`. `public_reply_queued` is the queue. `public_reply_confirmed` is a normal public reply sent. `public_fallback_confirmed` is a fallback sent. `status` prints lines like `stat day=... event=... count=...`.

~~~bash
python -m comment_to_inbox --config config/project.json stats --format csv
python -m comment_to_inbox --config config/project.json stats --day 2026-01-01 --format csv
~~~

By default the previous closed UTC day is exported. The CSV has a header and one project row. Missing events are zeros. `runs_started` and `leads` refer to runs of the selected day. Leads are joined by `run_id`, so an address received later is counted in the day the run started. `conversion` is `leads / runs_started`. `preliminary=yes` means some of those runs are still waiting for a reply.

## Possible extensions

**A reminder when the person is silent.** `silence_reminder_allowed` in `comment_to_inbox/followup.py` only checks the conditions. A reminder is allowed when the run is still waiting for an email, the person has not opted out, and their own DM opened a window that is still open. Silence after the first private reply does not open that window by itself.

**Reusing an address.** Skipping the email ask when an address was already collected needs a separate product decision. The person's expectations, the retention period, what makes an address stale, and how deletion works all have to be defined. An opt-out blocks reuse.

## What the tests check

| Module | Scenario |
| --- | --- |
| `test_scenario.py` | The full path from comment to link |
| `test_dedup.py`, `test_email_race.py` | Repeated events, comments on another post, and a race of two addresses |
| `test_rerequest.py`, `test_stop.py` | Three re-asks, exhaustion, and opt-out |
| `test_rate_limit.py`, `test_recovery.py` | Budget, retries, timeout, restart, and sequential sending |
| `test_early_message.py`, `test_followup.py` | Early DMs, platform time, and the reminder-condition check |
| `test_leads.py`, `test_notify.py` | Independent queues, claim recovery, and notices without message text |
| `test_routing.py`, `test_product_rules.py` | List precedence, dry run, old configs and volumes, disabled posts, wait expiry, opt-out, and first-DM outcomes |
| `test_zernio.py` | Three stubbed Inbox calls, thread matching, and error classification |

## AI usage

- Codex GPT-6.1 Sol was used to brainstorm and to review existing approaches.
- I decided the architecture, the product assumptions, and which services to use.
- Cursor Grok was used to generate code. I reviewed the edits by hand in the important places.
- Codex and I debugged and tested the solution at different stages.
- I wrote the first draft of the README.
- Codex was used to expand the README and polish the wording.
- After that I did a final read-through of the whole project and manual tests on live accounts.

## License

MIT. The notice names the comment-to-inbox authors as the copyright holders.
