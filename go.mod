module github.com/elastic/elastic-agent-shipper

go 1.17

require (
	github.com/elastic/elastic-agent-libs v0.2.5
	github.com/spf13/cobra v1.3.0
	google.golang.org/genproto v0.0.0-20220614165028-45ed7f3ff16e
	google.golang.org/grpc v1.47.0
	google.golang.org/protobuf v1.28.0
)

require (
	github.com/elastic/beats/v7 v7.0.0-alpha2.0.20220614164757-2b44bb8da62b
	github.com/elastic/elastic-agent v0.0.0-20220330154707-da786a47a0c5
	github.com/elastic/elastic-agent-client/v7 v7.0.0-20220607160924-1a71765a8bbe
	github.com/magefile/mage v1.13.0
	github.com/stretchr/testify v1.7.0
	go.elastic.co/go-licence-detector v0.5.0
	golang.org/x/net v0.0.0-20220614195744-fb05da6f9022
)

require (
	github.com/akavel/rsrc v0.10.2 // indirect
	github.com/armon/go-radix v1.0.0 // indirect
	github.com/cyphar/filepath-securejoin v0.2.3 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/elastic/go-licenser v0.4.0 // indirect
	github.com/elastic/go-structform v0.0.9 // indirect
	github.com/elastic/go-sysinfo v1.8.0 // indirect
	github.com/elastic/go-ucfg v0.8.5 // indirect
	github.com/elastic/go-windows v1.0.1 // indirect
	github.com/fatih/color v1.13.0 // indirect
	github.com/gobuffalo/here v0.6.0 // indirect
	github.com/gofrs/uuid v4.2.0+incompatible // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/licenseclassifier v0.0.0-20200402202327-879cb1424de0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/jcchavezs/porto v0.4.0 // indirect
	github.com/joeshaw/multierror v0.0.0-20140124173710-69b34d4ec901 // indirect
	github.com/josephspurrier/goversioninfo v1.4.0 // indirect
	github.com/karrick/godirwalk v1.15.8 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/markbates/pkger v0.17.0 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/mitchellh/hashstructure v1.1.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/santhosh-tekuri/jsonschema v1.2.4 // indirect
	github.com/sergi/go-diff v1.1.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	go.elastic.co/apm/v2 v2.1.0 // indirect
	go.elastic.co/ecszap v1.0.1 // indirect
	go.elastic.co/fastjson v1.1.0 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/goleak v1.1.12 // indirect
	go.uber.org/multierr v1.8.0 // indirect
	go.uber.org/zap v1.21.0 // indirect
	golang.org/x/crypto v0.0.0-20220525230936-793ad666bf5e // indirect
	golang.org/x/mod v0.6.0-dev.0.20220419223038-86c51ed26bb4 // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c // indirect
	golang.org/x/sys v0.0.0-20220614162138-6c1b26c55098 // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/tools v0.1.11 // indirect
	golang.org/x/xerrors v0.0.0-20220609144429-65e65417b02f // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	howett.net/plist v1.0.0 // indirect
)

replace (
	github.com/dop251/goja => github.com/andrewkroh/goja v0.0.0-20190128172624-dd2ac4456e20
	github.com/dop251/goja_nodejs => github.com/dop251/goja_nodejs v0.0.0-20171011081505-adff31b136e6
	github.com/elastic/elastic-agent => ../elastic-agent
)
