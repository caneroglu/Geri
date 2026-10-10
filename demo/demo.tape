# Renders demo/demo.gif. Run from the repo root: vhs demo/demo.tape
# Needs: vhs, tmux, jq, curl.

Output demo/demo.gif

Set Shell "bash"
Set FontFamily "JetBrainsMono Nerd Font"
Set FontSize 15
Set Width 1600
Set Height 620
Set Padding 16
Set Theme "Catppuccin Mocha"
Set TypingSpeed 55ms

Hide
Type `source demo/env.sh && clear` Enter
Sleep 8s
Type `tmux -f /dev/null new-session -c "$PWD" \; set status off \; split-window -h -c "$PWD"` Enter
Sleep 1s
Show

# Right pane: follow the log live.
Type `curl -sN "$H/v4/stream?source=nasa-sat-01" -H "$AUTH" | sed -un 's/^data: //p' | jq -c '.records[] | {cursor, entity_id, data}'` Enter
Sleep 1s

# Left pane: the source declares itself, then sends records.
Ctrl+B
Type "o"
Sleep 500ms
Type `curl -s -X PUT $H/v4/sources/nasa-sat-01 -H "$AUTH" -H 'content-type: application/json' -d @source.json -w '%{http_code}\n'` Enter
Sleep 1.5s

Type `curl -s $H/v4/batch -H "$AUTH" -d @1.json | jq -c '.records[]'` Enter
Sleep 3s
Type `curl -s $H/v4/batch -H "$AUTH" -d @2.json | jq -c '.records[]'` Enter
Sleep 3s

# Same batch again: idempotent, nothing new on the right.
Type `curl -s $H/v4/batch -H "$AUTH" -d @2.json | jq -c '.records[]'` Enter
Sleep 3.5s

# Sequence 4 before 3: the hub reports the gap.
Type `curl -s $H/v4/batch -H "$AUTH" -d @4.json | jq -c '.records[]'` Enter
Sleep 3.5s
Type `curl -s $H/v4/batch -H "$AUTH" -d @3.json | jq -c '.records[]'` Enter
Sleep 3s

Type `curl -s $H/v4/state/nasa-sat-01 -H "$AUTH" | jq -c '.[] | {entity_id, data}'` Enter
Sleep 5s

Hide
Ctrl+B
Type ":kill-server"
Enter
