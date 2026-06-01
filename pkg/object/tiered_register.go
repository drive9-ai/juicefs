package object

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
)

const tieredObjectStoreExperimentalEnv = "JUICEFS_TIERED_OBJECT_STORE_EXPERIMENTAL"

type tieredObjectStoreEndpointConfig struct {
	Enabled             bool
	IntegrationVerified bool
	SQLDialect          tieredSQLDialect
	SQLDSNEnv           string
	VolumeID            []byte
	SmallThreshold      int64
	LargeStorage        string
	LargeEndpoint       string
}

func init() {
	Register("tiered", newTieredObjectStoreFromEndpoint)
}

func newTieredObjectStoreFromEndpoint(endpoint, accessKey, secretKey, token string) (ObjectStorage, error) {
	cfg, err := parseTieredObjectStoreEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	if err := requireTieredObjectStoreRuntimeEnable(cfg); err != nil {
		return nil, err
	}
	if err := validateTieredObjectStoreEndpointConfig(cfg); err != nil {
		return nil, err
	}
	sqlDSN := os.Getenv(cfg.SQLDSNEnv)
	if strings.TrimSpace(sqlDSN) == "" {
		return nil, fmt.Errorf("%w: empty SQL DSN from env %q", errTieredStoreInvalidConfig, cfg.SQLDSNEnv)
	}

	db, err := sql.Open(string(cfg.SQLDialect), removeTieredSQLScheme(sqlDSN))
	if err != nil {
		return nil, err
	}
	large, err := CreateStorage(cfg.LargeStorage, cfg.LargeEndpoint, accessKey, secretKey, token)
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	store, err := newTieredObjectStoreFromConfig(context.Background(), db, tieredObjectStoreConfig{
		Enabled:        true,
		SQLDialect:     cfg.SQLDialect,
		VolumeID:       cfg.VolumeID,
		SmallThreshold: cfg.SmallThreshold,
	}, large)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func parseTieredObjectStoreEndpoint(endpoint string) (tieredObjectStoreEndpointConfig, error) {
	uri, err := url.Parse(endpoint)
	if err != nil {
		return tieredObjectStoreEndpointConfig{}, fmt.Errorf("%w: invalid tiered endpoint: %v", errTieredStoreInvalidConfig, err)
	}
	if uri.Scheme != "" && uri.Scheme != "tiered" {
		return tieredObjectStoreEndpointConfig{}, fmt.Errorf("%w: invalid tiered endpoint scheme %q", errTieredStoreInvalidConfig, uri.Scheme)
	}
	values := uri.Query()
	if _, ok := values["sql_dsn"]; ok {
		return tieredObjectStoreEndpointConfig{}, fmt.Errorf("%w: sql_dsn query parameter is not allowed", errTieredStoreInvalidConfig)
	}

	enabled, err := tieredBoolParam(values, "experimental")
	if err != nil {
		return tieredObjectStoreEndpointConfig{}, err
	}

	threshold, err := tieredInt64Param(values, "small_threshold")
	if err != nil {
		return tieredObjectStoreEndpointConfig{}, err
	}

	volumeID := values.Get("volume_id")
	if volumeID == "" {
		volumeID = strings.TrimPrefix(uri.Host+uri.Path, "/")
	}

	return tieredObjectStoreEndpointConfig{
		Enabled:             enabled,
		IntegrationVerified: values.Get("integration") == "verified",
		SQLDialect:          tieredSQLDialect(values.Get("sql_dialect")),
		SQLDSNEnv:           values.Get("sql_dsn_env"),
		VolumeID:            []byte(volumeID),
		SmallThreshold:      threshold,
		LargeStorage:        values.Get("large_storage"),
		LargeEndpoint:       values.Get("large_endpoint"),
	}, nil
}

func requireTieredObjectStoreRuntimeEnable(cfg tieredObjectStoreEndpointConfig) error {
	if !cfg.Enabled {
		return errTieredStoreExperimental
	}
	if !tieredEnvEnabled(os.Getenv(tieredObjectStoreExperimentalEnv)) {
		return errTieredStoreExperimental
	}
	if !cfg.IntegrationVerified {
		return errTieredStoreExperimental
	}
	return nil
}

func validateTieredObjectStoreEndpointConfig(cfg tieredObjectStoreEndpointConfig) error {
	if !isTieredObjectStoreDialectSupported(cfg.SQLDialect) {
		return fmt.Errorf("%w: unsupported SQL dialect %q", errTieredStoreInvalidConfig, cfg.SQLDialect)
	}
	if strings.TrimSpace(cfg.SQLDSNEnv) == "" {
		return fmt.Errorf("%w: empty SQL DSN env", errTieredStoreInvalidConfig)
	}
	if !isTieredEnvName(cfg.SQLDSNEnv) {
		return fmt.Errorf("%w: invalid SQL DSN env %q", errTieredStoreInvalidConfig, cfg.SQLDSNEnv)
	}
	if len(cfg.VolumeID) == 0 {
		return fmt.Errorf("%w: empty volume ID", errTieredStoreInvalidConfig)
	}
	if cfg.SmallThreshold <= 0 {
		return fmt.Errorf("%w: small threshold must be positive", errTieredStoreInvalidConfig)
	}
	if strings.TrimSpace(cfg.LargeStorage) == "" {
		return fmt.Errorf("%w: empty large storage", errTieredStoreInvalidConfig)
	}
	if cfg.LargeStorage == "tiered" {
		return fmt.Errorf("%w: tiered large storage recursion", errTieredStoreInvalidConfig)
	}
	if strings.TrimSpace(cfg.LargeEndpoint) == "" {
		return fmt.Errorf("%w: empty large endpoint", errTieredStoreInvalidConfig)
	}
	return nil
}

func tieredBoolParam(values url.Values, key string) (bool, error) {
	value := values.Get(key)
	if value == "" {
		return false, nil
	}
	enabled, ok := parseTieredBool(value)
	if !ok {
		return false, fmt.Errorf("%w: invalid %s %q", errTieredStoreInvalidConfig, key, value)
	}
	return enabled, nil
}

func tieredInt64Param(values url.Values, key string) (int64, error) {
	value := values.Get(key)
	if value == "" {
		return 0, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid %s %q", errTieredStoreInvalidConfig, key, value)
	}
	return parsed, nil
}

func parseTieredBool(value string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "on":
		return true, true
	case "0", "false", "no", "off":
		return false, true
	default:
		return false, false
	}
}

func tieredEnvEnabled(value string) bool {
	enabled, ok := parseTieredBool(value)
	return ok && enabled
}

func isTieredEnvName(value string) bool {
	if value == "" {
		return false
	}
	for i, r := range value {
		if r == '_' || r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' {
			continue
		}
		if i > 0 && r >= '0' && r <= '9' {
			continue
		}
		return false
	}
	return true
}

func removeTieredSQLScheme(dsn string) string {
	p := strings.Index(dsn, "://")
	if p > 0 {
		return dsn[p+3:]
	}
	return dsn
}
