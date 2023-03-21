go env -w GOPRIVATE=github.com/dyng/nossence-algo

gh_token=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
cat <<EOF > ~/.netrc
machine github.com
login oauth2
password ${gh_token}
EOF

go get -u github.com/dyng/nossence-algo
