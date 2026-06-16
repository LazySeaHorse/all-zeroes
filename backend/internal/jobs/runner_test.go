package jobs

import "testing"

func TestChunkName(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		noChunk  bool
		idx      int
		want     string
	}{
		{name: "first part", filename: "movie.mkv", idx: 0, want: "movie.mkv.part01"},
		{name: "second part", filename: "movie.mkv", idx: 1, want: "movie.mkv.part02"},
		{name: "no chunk preserves filename", filename: "movie.mkv", noChunk: true, idx: 0, want: "movie.mkv"},
		{name: "sanitizes path", filename: "../movie.mkv", idx: 0, want: "movie.mkv.part01"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ChunkName(tt.filename, tt.noChunk, tt.idx); got != tt.want {
				t.Fatalf("ChunkName() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestChunkURLEscapesName(t *testing.T) {
	got := ChunkURL("https://nc.example/public.php/webdav/", "my movie #1.mkv", false, 0)
	want := "https://nc.example/public.php/webdav/my%20movie%20%231.mkv.part01"
	if got != want {
		t.Fatalf("ChunkURL() = %q, want %q", got, want)
	}
}
