#!/bin/bash
set -e

precommit_hookfile="./.git/hooks/pre-commit"

echo -n "Write pre-commit hook to $precommit_hookfile (Y/N)? "
read response

if [ "$response" != "Y" ]; then
  exit 1
fi

cat <<\FIN > "$precommit_hookfile"
#!/bin/bash

if git rev-parse --verify HEAD >/dev/null 2>&1
then
	against=HEAD
else
	# Initial commit: diff against an empty tree object
	against=$(git hash-object -t tree /dev/null)
fi

# If you want to allow possible secrets set this variable to true.
allowpossiblesecrets=$(git config --type=bool hooks.allowpossiblesecrets)

# Redirect output to stderr.
exec 1>&2

UUIDregex='[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
GitHubPATregex='ghp_[a-zA-Z0-9]{36}'

forbiddenRegexes=(\
  "$UUIDregex"\
  "$GitHubPATregex"\
)
forbiddenRegexDescriptions=(\
  "UUID"\
  "GitHub Personal Authentication Token"\
)

for i in "${!forbiddenRegexes[@]}"; do
  diffRes=$(git diff --cached --diff-filter=AM -G "${forbiddenRegexes[i]}" -z $against)
  diffNames=$(git diff --name-only --cached --diff-filter=AM -G "${forbiddenRegexes[i]}" -z $against)

  if [ "$allowpossiblesecrets" != "true" ] && test -n "$diffRes"
  then
  	cat <<EOF
Error: Attempt to make commit containing possible secrets.

The following files may contain a ${forbiddenRegexDescriptions[i]}:
$diffNames

If you know what you are doing you can disable this check using:

  git config hooks.allowpossiblesecrets
EOF
  	exit 1
  fi
done

exit 0
FIN

chmod +x $precommit_hookfile

