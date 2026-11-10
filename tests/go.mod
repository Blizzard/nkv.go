module github.com/blizzard/nkv.go/tests

go 1.27

replace github.com/blizzard/nkv.go => ../

require (
	github.com/blizzard/nkv.go v0.0.0-00010101000000-000000000000
	github.com/matryer/is v1.4.1
	github.com/nats-io/nats-server/v2 v2.15.2
	github.com/nats-io/nats.go v1.55.0
)

require (
	github.com/antithesishq/antithesis-sdk-go v0.8.0 // indirect
	github.com/google/go-tpm v0.9.8 // indirect
	github.com/klauspost/compress v1.20.4 // indirect
	github.com/minio/highwayhash v1.0.4 // indirect
	github.com/nats-io/jwt/v2 v2.8.3 // indirect
	github.com/nats-io/nkeys v0.4.17 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/sys v0.50.0 // indirect
	golang.org/x/time v0.16.0 // indirect
)
