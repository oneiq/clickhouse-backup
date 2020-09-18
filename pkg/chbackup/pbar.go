package chbackup

import (
	"io"

	progressbar "gopkg.in/cheggaaa/pb.v1"
)

type Bar struct {
	pb   *progressbar.ProgressBar
	show bool
}

func StartNewByteBar(show bool, total int64) *Bar {
	if show {
		return &Bar{
			show: true,
			pb:   progressbar.StartNew(int(total)).SetUnits(progressbar.U_BYTES),
		}
	}
	return &Bar{
		show: false,
	}
}

func StartNewBar(show bool, total int) *Bar {
	if show {
		return &Bar{
			show: true,
			pb:   progressbar.StartNew(total),
		}
	}
	return &Bar{
		show: false,
	}
}

func (b *Bar) Finish() {
	if b.show {
		b.pb.Finish()
	}
}

func (b *Bar) Add64(add int64) {
	if b.show {
		b.pb.Add64(add)
	}
}

func (b *Bar) Set(current int) {
	if b.show {
		b.pb.Set(current)
	}
}

func (b *Bar) Increment() {
	if b.show {
		b.pb.Increment()
	}
}

func (b *Bar) SetTotal64(total int64) {
	if b.show && total != 0 {
		b.pb.SetTotal64(total)
		// safe to turn these on now
		b.pb.AutoStat = true
		b.pb.ShowPercent = true
		b.pb.ShowTimeLeft = true
	}
}

func (b *Bar) NewProxyReader(r io.Reader) io.Reader {
	if b.show {
		return b.pb.NewProxyReader(r)
	}
	return r
}
