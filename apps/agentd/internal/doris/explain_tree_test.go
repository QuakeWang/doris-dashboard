package doris

import "testing"

func TestBuildExplainTreeQuery(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{name: "plain sql", in: "select 1", want: "EXPLAIN TREE select 1"},
		{name: "plain sql with semicolon", in: "select 1;", want: "EXPLAIN TREE select 1"},
		{name: "explain select", in: "explain select 1", want: "EXPLAIN TREE select 1"},
		{name: "explain tree", in: "EXPLAIN TREE select 1", want: "EXPLAIN TREE select 1"},
		{name: "explain analyzed", in: "EXPLAIN ANALYZED select 1", want: "EXPLAIN ANALYZED TREE select 1"},
		{
			name: "explain analyzed tree",
			in:   "EXPLAIN ANALYZED TREE select 1",
			want: "EXPLAIN ANALYZED TREE select 1",
		},
		{
			name: "explain with hint comment",
			in:   "EXPLAIN /*+ SET_VAR(enable_nereids_planner=true) */ select 1",
			want: "EXPLAIN TREE /*+ SET_VAR(enable_nereids_planner=true) */ select 1",
		},
		{name: "explain graph rejected", in: "EXPLAIN GRAPH select 1", wantErr: true},
		{name: "explain process rejected", in: "EXPLAIN PROCESS select 1", wantErr: true},
		{name: "explain tree process rejected", in: "EXPLAIN TREE PROCESS select 1", wantErr: true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := buildExplainTreeQuery(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (result=%q)", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("unexpected result:\nwant: %q\ngot:  %q", tc.want, got)
			}
		})
	}
}

func TestParseLeadingUseDatabase(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		in       string
		wantDB   string
		wantRest string
		wantOk   bool
		wantErr  bool
	}{
		{
			name:     "no use",
			in:       "select 1",
			wantOk:   false,
			wantRest: "select 1",
		},
		{
			name:     "use select",
			in:       "use tpch; select 1",
			wantOk:   true,
			wantDB:   "tpch",
			wantRest: "select 1",
		},
		{
			name:     "use explain",
			in:       "USE tpch; EXPLAIN select 1",
			wantOk:   true,
			wantDB:   "tpch",
			wantRest: "EXPLAIN select 1",
		},
		{
			name:     "quoted db",
			in:       "use `db-prod`; select 1;",
			wantOk:   true,
			wantDB:   "db-prod",
			wantRest: "select 1;",
		},
		{
			name:     "keyword prefix not match",
			in:       "useful select 1",
			wantOk:   false,
			wantRest: "useful select 1",
		},
		{
			name:    "missing db",
			in:      "use ; select 1",
			wantOk:  true,
			wantErr: true,
		},
		{
			name:    "missing semicolon",
			in:      "use tpch select 1",
			wantOk:  true,
			wantErr: true,
		},
		{
			name:    "no sql after use",
			in:      "use tpch;  ",
			wantOk:  true,
			wantErr: true,
		},
		{
			name:    "invalid unquoted name",
			in:      "use tpch-1; select 1",
			wantOk:  true,
			wantErr: true,
		},
		{
			name:     "quoted name allows dash",
			in:       "use `tpch-1`; select 1",
			wantOk:   true,
			wantDB:   "tpch-1",
			wantRest: "select 1",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotDB, gotRest, gotOk, err := parseLeadingUseDatabase(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (db=%q rest=%q ok=%v)", gotDB, gotRest, gotOk)
				}
				if gotOk != tc.wantOk {
					t.Fatalf("unexpected ok:\nwant: %v\ngot:  %v", tc.wantOk, gotOk)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if gotOk != tc.wantOk {
				t.Fatalf("unexpected ok:\nwant: %v\ngot:  %v", tc.wantOk, gotOk)
			}
			if gotDB != tc.wantDB {
				t.Fatalf("unexpected db:\nwant: %q\ngot:  %q", tc.wantDB, gotDB)
			}
			if gotRest != tc.wantRest {
				t.Fatalf("unexpected rest:\nwant: %q\ngot:  %q", tc.wantRest, gotRest)
			}
		})
	}
}
