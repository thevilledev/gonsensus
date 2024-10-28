package gonsensus

import "testing"

func TestNewManager(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		client      S3Client
		bucket      string
		cfg         Config
		expectError bool
	}{
		{
			name:        "Valid configuration",
			client:      NewMockS3Client(),
			bucket:      "test-bucket",
			cfg:         Config{},
			expectError: false,
		},
		{
			name:        "Missing client",
			client:      nil,
			bucket:      "test-bucket",
			cfg:         Config{},
			expectError: true,
		},
		{
			name:        "Missing bucket",
			client:      NewMockS3Client(),
			bucket:      "",
			cfg:         Config{},
			expectError: true,
		},
	}

	for _, tCase := range tests {
		t.Run(tCase.name, func(t *testing.T) {
			t.Parallel()

			manager, err := NewManager(tCase.client, tCase.bucket, tCase.cfg)

			if tCase.expectError {
				if err == nil {
					t.Error("expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}

				if manager == nil {
					t.Error("expected manager, got nil")
				}
			}
		})
	}
}
