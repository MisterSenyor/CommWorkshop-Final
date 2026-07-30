#include "mock_transport.h"
#include "ring_collectives.h"

#include <cmath>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <vector>

#define CHECK(x) do { if (!(x)) throw std::runtime_error(#x); } while (0)

template<class T>
void run_case(int p, std::size_t count, DATATYPE dt, OPERATION op,
              std::size_t piece_bytes, unsigned max_inflight,
              T (*input)(int,std::size_t), T (*expected)(int,std::size_t),
              double tol=0.0) {
    pg_transport_t** transports = nullptr;
    CHECK(mock_transport_group_create(p, &transports) == 0);
    std::vector<std::vector<T>> in(p, std::vector<T>(count));
    std::vector<std::vector<T>> out(p, std::vector<T>(count));
    std::vector<int> status(p, -999);
    for (int r=0;r<p;++r) for(std::size_t i=0;i<count;++i) in[r][i]=input(r,i);
    std::vector<std::thread> th;
    for (int r=0;r<p;++r) th.emplace_back([&,r]{
        ring_collective_config_t cfg{piece_bytes,max_inflight,PG_TRANSFER_AUTO};
        ring_collective_context_t* ctx=nullptr;
        int s=ring_collective_create(transports[r],&cfg,&ctx);
        if(s==PG_SUCCESS) s=ring_all_reduce(ctx,in[r].data(),out[r].data(),count,dt,op);
        status[r]=s;
        ring_collective_destroy(ctx);
    });
    for(auto& t:th)t.join();
    for(int r=0;r<p;++r){
        CHECK(status[r]==PG_SUCCESS);
        for(std::size_t i=0;i<count;++i){
            double a=(double)out[r][i], e=(double)expected(p,i);
            if(std::fabs(a-e)>tol){
                std::cerr << "Mismatch p="<<p<<" rank="<<r<<" i="<<i<<" actual="<<a<<" expected="<<e<<"\n";
                throw std::runtime_error("result mismatch");
            }
        }
    }
    mock_transport_group_destroy(transports,p);
}

std::int32_t int_sum_input(int r,std::size_t i){return (r+1)*100+(std::int32_t)i;}
std::int32_t int_sum_expected(int p,std::size_t i){return 100*p*(p+1)/2 + p*(std::int32_t)i;}
float float_sum_input(int r,std::size_t i){return 0.5f*(r+1)+(float)i;}
float float_sum_expected(int p,std::size_t i){return 0.25f*p*(p+1)+p*(float)i;}
double double_max_input(int r,std::size_t i){return 100.0*r+(double)i;}
double double_max_expected(int p,std::size_t i){return 100.0*(p-1)+(double)i;}

void direct_phase_case(int p, std::size_t count){
    pg_transport_t** transports=nullptr; CHECK(mock_transport_group_create(p,&transports)==0);
    std::vector<std::vector<std::int32_t>> work(p,std::vector<std::int32_t>(count));
    for(int r=0;r<p;++r)for(std::size_t i=0;i<count;++i)work[r][i]=int_sum_input(r,i);
    std::vector<int> status(p), owned(p,-1); std::vector<std::thread> th;
    for(int r=0;r<p;++r) th.emplace_back([&,r]{
        ring_collective_config_t cfg{4,2,PG_TRANSFER_AUTO}; ring_collective_context_t* ctx=nullptr;
        int s=ring_collective_create(transports[r],&cfg,&ctx);
        if(s==PG_SUCCESS) s=ring_reduce_scatter_inplace(ctx,work[r].data(),count,PG_INT32,PG_SUM,&owned[r]);
        if(s==PG_SUCCESS) s=ring_all_gather_inplace(ctx,work[r].data(),count,PG_INT32,owned[r]);
        status[r]=s; ring_collective_destroy(ctx);
    });
    for(auto& t:th)t.join();
    for(int r=0;r<p;++r){CHECK(status[r]==PG_SUCCESS);CHECK(owned[r]==(r+1)%p);for(std::size_t i=0;i<count;++i)CHECK(work[r][i]==int_sum_expected(p,i));}
    mock_transport_group_destroy(transports,p);
}

int main(){
    try{
        run_case<std::int32_t>(2,4,PG_INT32,PG_SUM,4,2,int_sum_input,int_sum_expected);
        std::cout << "PASS 2-process int SUM\n";
        run_case<std::int32_t>(4,1003,PG_INT32,PG_SUM,16,4,int_sum_input,int_sum_expected);
        std::cout << "PASS 4-process uneven pipelined int SUM\n";
        run_case<float>(4,257,PG_FLOAT32,PG_SUM,12,3,float_sum_input,float_sum_expected,1e-4);
        std::cout << "PASS 4-process float SUM\n";
        run_case<double>(4,65,PG_FLOAT64,PG_MAX,24,2,double_max_input,double_max_expected,1e-12);
        std::cout << "PASS 4-process double MAX\n";
        direct_phase_case(2,17); std::cout << "PASS direct phases 2 processes\n";
        direct_phase_case(4,19); std::cout << "PASS direct phases 4 processes\n";
        std::cout << "ALL LOCAL COLLECTIVE TESTS PASSED\n";
        return 0;
    }catch(const std::exception& e){std::cerr<<"FAIL: "<<e.what()<<"\n";return 1;}
}
