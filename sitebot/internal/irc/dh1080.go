package irc

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"math/big"
	"strings"
)

var (
	dh1080Generator = big.NewInt(2)
	dh1080Prime, _  = new(big.Int).SetString(
		"FBE1022E23D213E8ACFA9AE8B9DFADA3EA6B7AC7A7B7E95AB5EB2DF858921FEADE95E6AC7BE7DE6ADBAB8A783E7AF7A7FA6A2B7BEB1E72EAE2B72F9FA2BFB2A2EFBEFAC868BADB3E828FA8BADFADA3E4CC1BE7E8AFE85E9698A783EB68FA07A77AB6AD7BEB618ACF9CA2897EB28A6189EFA07AB99A8A7FA9AE299EFA7BA66DEAFEFBEFBF0B7D8B",
		16,
	)
	dh1080PrimeMinus1 = new(big.Int).Sub(new(big.Int).Set(dh1080Prime), big.NewInt(1))
	dh1080PrimeQ      = new(big.Int).Rsh(new(big.Int).Set(dh1080PrimeMinus1), 1)
)

type DH1080Ctx struct {
	private *big.Int
	public  *big.Int
	secret  *big.Int
	mode    string
}

func NewDH1080Ctx(preferredMode string) (*DH1080Ctx, error) {
	mode := strings.ToUpper(strings.TrimSpace(preferredMode))
	if mode != "ECB" {
		mode = "CBC"
	}
	for {
		privBytes := make([]byte, 135)
		if _, err := rand.Read(privBytes); err != nil {
			return nil, err
		}
		private := new(big.Int).SetBytes(privBytes)
		if private.Sign() <= 0 {
			continue
		}
		public := new(big.Int).Exp(dh1080Generator, private, dh1080Prime)
		if public.Cmp(big.NewInt(2)) < 0 || public.Cmp(dh1080PrimeMinus1) > 0 {
			continue
		}
		if new(big.Int).Exp(public, dh1080PrimeQ, dh1080Prime).Cmp(big.NewInt(1)) != 0 {
			continue
		}
		return &DH1080Ctx{
			private: private,
			public:  public,
			mode:    mode,
		}, nil
	}
}

func (c *DH1080Ctx) InitMessage() string {
	return "DH1080_INIT " + dh1080B64Encode(intToBytes(c.public)) + " " + c.mode
}

func (c *DH1080Ctx) FinishMessage() string {
	return "DH1080_FINISH " + dh1080B64Encode(intToBytes(c.public)) + " " + c.mode
}

func (c *DH1080Ctx) Handle(msg string) error {
	_, publicRaw, mode, err := parseDH1080Message(msg)
	if err != nil {
		return err
	}
	public := bytesToInt(dh1080B64Decode(publicRaw))
	if public == nil || public.Cmp(big.NewInt(1)) <= 0 || public.Cmp(dh1080Prime) >= 0 {
		return fmt.Errorf("invalid DH1080 public key")
	}
	c.secret = new(big.Int).Exp(public, c.private, dh1080Prime)
	if mode == "ECB" || mode == "CBC" {
		c.mode = mode
	}
	return nil
}

func (c *DH1080Ctx) NegotiatedKey() (string, error) {
	if c.secret == nil || c.secret.Sign() == 0 {
		return "", fmt.Errorf("DH1080 secret not established")
	}
	sum := sha256.Sum256(intToBytes(c.secret))
	secret := dh1080B64Encode(sum[:])
	if c.mode == "ECB" {
		return secret, nil
	}
	return "cbc:" + secret, nil
}

func parseDH1080Message(msg string) (command, public, mode string, err error) {
	fields := strings.Fields(strings.TrimSpace(msg))
	if len(fields) < 2 {
		return "", "", "", fmt.Errorf("invalid DH1080 message")
	}
	command = strings.ToUpper(fields[0])
	if command != "DH1080_INIT" && command != "DH1080_FINISH" {
		return "", "", "", fmt.Errorf("invalid DH1080 command")
	}
	public = fields[1]
	mode = "CBC"
	if len(fields) >= 3 {
		maybeMode := strings.ToUpper(strings.TrimSpace(fields[2]))
		if maybeMode == "ECB" || maybeMode == "CBC" {
			mode = maybeMode
		}
	}
	return command, public, mode, nil
}

func dh1080B64Encode(src []byte) string {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	out := make([]byte, 0, len(src)*2)
	var t byte
	j := 0
	m := byte(0x80)
	for i := 0; i < len(src)*8; i++ {
		if src[i>>3]&m != 0 {
			t |= 1
		}
		j++
		m >>= 1
		if m == 0 {
			m = 0x80
		}
		if j%6 == 0 {
			out = append(out, alphabet[t])
			t = 0
		}
		t <<= 1
	}
	if mod := j % 6; mod != 0 {
		t <<= 5 - mod
		out = append(out, alphabet[t])
	}
	return string(out)
}

func dh1080B64Decode(src string) []byte {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	if len(src) < 2 {
		return nil
	}
	index := make([]byte, 256)
	for i := 0; i < len(alphabet); i++ {
		index[alphabet[i]] = byte(i)
	}
	L := len(src)
	for i := L - 2; i >= 0; i-- {
		if index[src[i]] == 0 {
			L--
			continue
		}
		break
	}
	if L < 2 {
		return nil
	}
	dst := make([]byte, 0, L)
	i, k := 0, 0
	for {
		i++
		if k+1 >= L {
			break
		}
		v := index[src[k]] << 2
		k++
		if k < L {
			v |= index[src[k]] >> 4
		} else {
			break
		}
		dst = append(dst, v)

		i++
		if k+1 >= L {
			break
		}
		v = index[src[k]] << 4
		k++
		if k < L {
			v |= index[src[k]] >> 2
		} else {
			break
		}
		dst = append(dst, v)

		i++
		if k+1 >= L {
			break
		}
		v = index[src[k]] << 6
		k++
		if k < L {
			v |= index[src[k]]
		} else {
			break
		}
		dst = append(dst, v)
		k++
	}
	return dst
}

func intToBytes(n *big.Int) []byte {
	if n == nil {
		return nil
	}
	return n.Bytes()
}

func bytesToInt(b []byte) *big.Int {
	if len(b) == 0 {
		return nil
	}
	return new(big.Int).SetBytes(b)
}
