#include <random>
#include <vector>
#include <algorithm>
constexpr static int FOUR_K = 4096;
template <template <class...> class Dis, class IntType, class... Ts>
auto get(Dis<IntType, Ts...>& dis, const int n, double aligned_ratio)
{
    std::vector<IntType> a = dis.get(n * FOUR_K * (1 - aligned_ratio), false);
    {
        std::vector<IntType> b = dis.get(n * (aligned_ratio), true);
        a.insert(a.end(), b.begin(), b.end());
    }
    std::shuffle(a.begin(), a.end(), dis.get_rng());
    return a;
}

template<class IntType = unsigned long>
class uniform_distribution {
    std::mt19937 rng;
    std::uniform_int_distribution<int> dis;
public:
    std::mt19937 get_rng() const {
        return rng;
    }
    uniform_distribution(int from, int to): rng(std::random_device()()), dis(from, to) {}

    std::vector<IntType> get(const int n, bool four_k_multiply = false) {
        std::vector<IntType> v(n);
        for (auto& i: v) i = getNext(four_k_multiply);
        return v;
    }

    IntType getNext(bool four_k_multiply = false) {
        auto v = dis(rng);
        if (four_k_multiply) v *= FOUR_K;
        return v;
    }
};

/** Zipf-like random distribution.
 *
 * "Rejection-inversion to generate variates from monotone discrete
 * distributions", Wolfgang Hörmann and Gerhard Derflinger
 * ACM TOMACS 6.3 (1996): 169-184
 */
template<class IntType = unsigned long, class RealType = double>
class zipf_distribution
{
    std::mt19937 rng;
public:
    std::mt19937 get_rng() const {
        return rng;
    }

    typedef RealType input_type;
    typedef IntType result_type;

    static_assert(std::numeric_limits<IntType>::is_integer, "");
    static_assert(!std::numeric_limits<RealType>::is_integer, "");

    zipf_distribution(const IntType n=std::numeric_limits<IntType>::max(),
                      const RealType q=1.0)
        : n(n)
        , q(q)
        , H_x1(H(1.5) - 1.0)
        , H_n(H(n + 0.5))
        , dist(H_x1, H_n)
        , rng(std::random_device()())
    {

    }

    std::vector<IntType> get(const int n, bool four_k_multiply = false) {
        std::vector<IntType> v(n);
        for (auto& i: v) i = getNext(four_k_multiply);
        return v;
    }

    IntType getNext(bool four_k_multiply = false) { 
        while (true) {
            const RealType u = dist(rng);
            const RealType x = H_inv(u);
            IntType  k = clamp<IntType>(std::round(x), 1, n);
            if (u >= H(k + 0.5) - h(k)) {
                if (four_k_multiply) k *= FOUR_K;
                return k;
            }
        }
    }

    IntType operator()()
    {
        return getNext();
    }

private:
    /** Clamp x to [min, max]. */
    template<typename T>
    static constexpr T clamp(const T x, const T min, const T max)
    {
        return std::max(min, std::min(max, x));
    }

    /** exp(x) - 1 / x */
    static double
    expxm1bx(const double x)
    {
        return (std::abs(x) > epsilon)
            ? std::expm1(x) / x
            : (1.0 + x/2.0 * (1.0 + x/3.0 * (1.0 + x/4.0)));
    }

    /** H(x) = log(x) if q == 1, (x^(1-q) - 1)/(1 - q) otherwise.
     * H(x) is an integral of h(x).
     *
     * Note the numerator is one less than in the paper order to work with all
     * positive q.
     */
    const RealType H(const RealType x)
    {
        const RealType log_x = std::log(x);
        return expxm1bx((1.0 - q) * log_x) * log_x;
    }

    /** log(1 + x) / x */
    static RealType
    log1pxbx(const RealType x)
    {
        return (std::abs(x) > epsilon)
            ? std::log1p(x) / x
            : 1.0 - x * ((1/2.0) - x * ((1/3.0) - x * (1/4.0)));
    }

    /** The inverse function of H(x) */
    const RealType H_inv(const RealType x)
    {
        const RealType t = std::max(-1.0, x * (1.0 - q));
        return std::exp(log1pxbx(t) * x);
    }

    /** That hat function h(x) = 1 / (x ^ q) */
    const RealType h(const RealType x)
    {
        return std::exp(-q * std::log(x));
    }

    static constexpr RealType epsilon = 1e-8;

    IntType                                  n;     ///< Number of elements
    RealType                                 q;     ///< Exponent
    RealType                                 H_x1;  ///< H(x_1)
    RealType                                 H_n;   ///< H(n)
    std::uniform_real_distribution<RealType> dist;  ///< [H(x_1), H(n)]
};


