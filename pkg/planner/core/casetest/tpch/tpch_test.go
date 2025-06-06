// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tpch

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/util/benchdaily"
	"github.com/stretchr/testify/require"
)

func TestQ1(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	createLineItem(t, tk, dom)
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_size = 0")
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_count = 0")
	integrationSuiteData := GetTPCHSuiteData()
	var (
		input  []string
		output []struct {
			SQL    string
			Result []string
		}
	)
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i := range input {
		testdata.OnRecord(func() {
			output[i].SQL = input[i]
		})
		testdata.OnRecord(func() {
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(input[i]).Rows())
		})
		tk.MustQuery(input[i]).Check(testkit.Rows(output[i].Result...))
	}
}

func TestQ2(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	createPart(t, tk, dom)
	createSupplier(t, tk, dom)
	createPartsupp(t, tk, dom)
	createNation(t, tk, dom)
	createRegion(t, tk, dom)
	testkit.LoadTableStats("test.part.json", dom)
	testkit.LoadTableStats("test.supplier.json", dom)
	testkit.LoadTableStats("test.partsupp.json", dom)
	testkit.LoadTableStats("test.region.json", dom)
	testkit.LoadTableStats("test.nation.json", dom)
	integrationSuiteData := GetTPCHSuiteData()
	var (
		input  []string
		output []struct {
			SQL    string
			Result []string
		}
	)
	integrationSuiteData.LoadTestCases(t, &input, &output)
	costTraceFormat := `explain format='cost_trace' `
	for i := range input {
		testdata.OnRecord(func() {
			output[i].SQL = input[i]
		})
		testdata.OnRecord(func() {
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(costTraceFormat + input[i]).Rows())
		})
		tk.MustQuery(costTraceFormat + input[i]).Check(testkit.Rows(output[i].Result...))
		checkCost(t, tk, input[i])
	}
}

func TestQ3(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	createCustomer(t, tk, dom)
	createOrders(t, tk, dom)
	createLineItem(t, tk, dom)
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_size = 0")
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_count = 0")
	integrationSuiteData := GetTPCHSuiteData()
	var (
		input  []string
		output []struct {
			SQL    string
			Result []string
		}
	)
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i := range input {
		testdata.OnRecord(func() {
			output[i].SQL = input[i]
		})
		testdata.OnRecord(func() {
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(input[i]).Rows())
		})
		tk.MustQuery(input[i]).Check(testkit.Rows(output[i].Result...))
	}
}

func TestQ4(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	createOrders(t, tk, dom)
	createLineItem(t, tk, dom)
	testkit.LoadTableStats("test.lineitem.json", dom)
	testkit.LoadTableStats("test.orders.json", dom)
	var (
		input  []string
		output []struct {
			SQL    string
			Result []string
		}
	)
	integrationSuiteData := GetTPCHSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	costTraceFormat := `explain format='cost_trace' `
	for i := range input {
		testdata.OnRecord(func() {
			output[i].SQL = input[i]
		})
		testdata.OnRecord(func() {
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(costTraceFormat + input[i]).Rows())
		})
		tk.MustQuery(costTraceFormat + input[i]).Check(testkit.Rows(output[i].Result...))
		checkCost(t, tk, input[i])
	}
}

func TestQ5(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	createCustomer(t, tk, dom)
	createOrders(t, tk, dom)
	createLineItem(t, tk, dom)
	createSupplier(t, tk, dom)
	createNation(t, tk, dom)
	createRegion(t, tk, dom)
	testkit.LoadTableStats("test.customer.json", dom)
	testkit.LoadTableStats("test.orders.json", dom)
	testkit.LoadTableStats("test.lineitem.json", dom)
	testkit.LoadTableStats("test.supplier.json", dom)
	testkit.LoadTableStats("test.nation.json", dom)
	testkit.LoadTableStats("test.region.json", dom)
	var (
		input  []string
		output []struct {
			SQL    string
			Result []string
		}
	)
	integrationSuiteData := GetTPCHSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	costTraceFormat := `explain format='cost_trace' `
	for i := range input {
		testdata.OnRecord(func() {
			output[i].SQL = input[i]
		})
		testdata.OnRecord(func() {
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(costTraceFormat + input[i]).Rows())
		})
		tk.MustQuery(costTraceFormat + input[i]).Check(testkit.Rows(output[i].Result...))
		checkCost(t, tk, input[i])
	}
}

// check the cost trace's cost and verbose's cost. they should be the same.
// it is from https://github.com/pingcap/tidb/issues/61155
func checkCost(t *testing.T, tk *testkit.TestKit, q4 string) {
	costTraceFormat := `explain format='cost_trace' `
	verboseFormat := `explain format='verbose' `
	costTraceRows := tk.MustQuery(costTraceFormat + q4)
	verboseRows := tk.MustQuery(verboseFormat + q4)
	require.Equal(t, len(costTraceRows.Rows()), len(verboseRows.Rows()))
	for i := 0; i < len(costTraceRows.Rows()); i++ {
		// check id / estRows / estCost. they should be the same one
		require.Equal(t, costTraceRows.Rows()[i][:3], verboseRows.Rows()[i][:3])
	}
}

func TestQ9(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	createLineItem(t, tk, dom)
	createNation(t, tk, dom)
	createOrders(t, tk, dom)
	createPart(t, tk, dom)
	createPartsupp(t, tk, dom)
	createSupplier(t, tk, dom)
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_size = 0")
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_count = 0")

	integrationSuiteData := GetTPCHSuiteData()
	var (
		input  []string
		output []struct {
			SQL    string
			Result []string
		}
	)
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i := range input {
		testdata.OnRecord(func() {
			output[i].SQL = input[i]
		})
		testdata.OnRecord(func() {
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(input[i]).Rows())
		})
		tk.MustQuery(input[i]).Check(testkit.Rows(output[i].Result...))
	}
}

func TestQ13(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	createCustomer(t, tk, dom)
	createOrders(t, tk, dom)
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_size = 0")
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_count = 0")
	integrationSuiteData := GetTPCHSuiteData()
	var (
		input  []string
		output []struct {
			SQL    string
			Result []string
		}
	)
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i := range input {
		testdata.OnRecord(func() {
			output[i].SQL = input[i]
		})
		testdata.OnRecord(func() {
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(input[i]).Rows())
		})
		tk.MustQuery(input[i]).Check(testkit.Rows(output[i].Result...))
	}
}

func TestQ18(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	createCustomer(t, tk, dom)
	createOrders(t, tk, dom)
	createLineItem(t, tk, dom)
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_size = 0")
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_count = 0")
	integrationSuiteData := GetTPCHSuiteData()
	var (
		input  []string
		output []struct {
			SQL    string
			Result []string
		}
	)
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i := range input {
		testdata.OnRecord(func() {
			output[i].SQL = input[i]
		})
		testdata.OnRecord(func() {
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(input[i]).Rows())
		})
		tk.MustQuery(input[i]).Check(testkit.Rows(output[i].Result...))
	}
}

func TestQ21(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	createSupplier(t, tk, dom)
	createLineItem(t, tk, dom)
	createOrders(t, tk, dom)
	createNation(t, tk, dom)
	var (
		input  []string
		output []struct {
			SQL    string
			Result []string
		}
	)
	integrationSuiteData := GetTPCHSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	costTraceFormat := `explain format='cost_trace' `
	for i := range input {
		testdata.OnRecord(func() {
			output[i].SQL = input[i]
		})
		testdata.OnRecord(func() {
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(costTraceFormat + input[i]).Rows())
		})
		tk.MustQuery(costTraceFormat + input[i]).Check(testkit.Rows(output[i].Result...))
		checkCost(t, tk, input[i])
	}
}

func TestQ22(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	createCustomer(t, tk, dom)
	createOrders(t, tk, dom)
	tk.MustExec("set @@tidb_opt_enable_non_eval_scalar_subquery=true")
	var (
		input  []string
		output []struct {
			SQL    string
			Result []string
		}
	)
	integrationSuiteData := GetTPCHSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	costTraceFormat := `explain format='cost_trace' `
	for i := range input {
		testdata.OnRecord(func() {
			output[i].SQL = input[i]
		})
		testdata.OnRecord(func() {
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(costTraceFormat + input[i]).Rows())
		})
		tk.MustQuery(costTraceFormat + input[i]).Check(testkit.Rows(output[i].Result...))
		checkCost(t, tk, input[i])
	}
}

func BenchmarkQ4(b *testing.B) {
	store, dom := testkit.CreateMockStoreAndDomain(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	createOrders(b, tk, dom)
	createLineItem(b, tk, dom)
	testkit.LoadTableStats("test.lineitem.json", dom)
	testkit.LoadTableStats("test.orders.json", dom)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		tk.MustQuery("SELECT o_orderpriority, COUNT(*) AS order_count FROM orders WHERE o_orderdate >= '1995-01-01' AND o_orderdate < DATE_ADD('1995-01-01', INTERVAL '3' MONTH) AND EXISTS (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate) GROUP BY o_orderpriority ORDER BY o_orderpriority;")
		tk.MustQuery("SELECT /*+ NO_INDEX_JOIN(orders, lineitem),NO_INDEX_HASH_JOIN(orders, lineitem) */ o_orderpriority, COUNT(*) AS order_count FROM orders WHERE o_orderdate >= '1995-01-01' AND o_orderdate < DATE_ADD('1995-01-01', INTERVAL '3' MONTH) AND EXISTS (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate) GROUP BY o_orderpriority ORDER BY o_orderpriority;")
	}
}

func TestBenchDaily(t *testing.T) {
	benchdaily.Run(
		BenchmarkQ4,
	)
}
