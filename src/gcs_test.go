package main

import "testing"

func TestUploadChunkSize(t *testing.T) {
	const kib = 1024
	cases := []struct {
		name string
		size int64
		want int
	}{
		{"empty file still gets a minimal buffer", 0, 256 * kib},
		{"small DICOM rounds up to one granule", 500 * kib, 512 * kib},
		{"exact multiple is unchanged", 512 * kib, 512 * kib},
		{"one byte over a granule rounds up", 512*kib + 1, 768 * kib},
		{"just under the cap rounds to the cap", 4*1024*kib - 1, 4 * 1024 * kib},
		{"large file is capped", 300 * 1024 * kib, 4 * 1024 * kib},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := uploadChunkSize(tc.size); got != tc.want {
				t.Fatalf("uploadChunkSize(%d) = %d, want %d", tc.size, got, tc.want)
			}
			if got := uploadChunkSize(tc.size); got%(256*kib) != 0 {
				t.Fatalf("uploadChunkSize(%d) = %d is not a multiple of 256KiB", tc.size, got)
			}
		})
	}
}
