package slothandler

import "sort"

type utilSample struct {
	active int
	cap    int
}

type utilWindow struct {
	samples []utilSample
	pos     int
	count   int
}

func newUtilWindow(size int) *utilWindow {
	if size <= 0 {
		size = 1
	}
	return &utilWindow{samples: make([]utilSample, size)}
}

func (u *utilWindow) Record(active, cap int) {
	if u == nil || len(u.samples) == 0 {
		return
	}
	u.samples[u.pos] = utilSample{active: active, cap: cap}
	u.pos = (u.pos + 1) % len(u.samples)
	if u.count < len(u.samples) {
		u.count++
	}
}

func (u *utilWindow) P90() float64 {
	if u == nil || u.count == 0 {
		return 0
	}
	values := make([]float64, 0, u.count)
	for i := 0; i < u.count; i++ {
		sample := u.samples[i]
		if sample.cap <= 0 {
			continue
		}
		values = append(values, float64(sample.active)/float64(sample.cap))
	}
	if len(values) == 0 {
		return 1
	}
	sort.Float64s(values)
	idx := (len(values)*9 + 9) / 10
	if idx <= 0 {
		idx = 1
	}
	if idx > len(values) {
		idx = len(values)
	}
	return values[idx-1]
}
