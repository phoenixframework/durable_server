defmodule DurableServer.UUID do
  @moduledoc false
  def uuid4 do
    <<u0::48, _::4, u1::12, _::2, u2::62>> = :crypto.strong_rand_bytes(16)
    encode(<<u0::48, 4::4, u1::12, 2::2, u2::62>>)
  end

  @hex {?0, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?a, ?b, ?c, ?d, ?e, ?f}
  defp encode(
         <<a1::4, a2::4, a3::4, a4::4, a5::4, a6::4, a7::4, a8::4, b1::4, b2::4, b3::4, b4::4,
           c1::4, c2::4, c3::4, c4::4, d1::4, d2::4, d3::4, d4::4, e1::4, e2::4, e3::4, e4::4,
           e5::4, e6::4, e7::4, e8::4, e9::4, e10::4, e11::4, e12::4>>
       ) do
    <<
      elem(@hex, a1),
      elem(@hex, a2),
      elem(@hex, a3),
      elem(@hex, a4),
      elem(@hex, a5),
      elem(@hex, a6),
      elem(@hex, a7),
      elem(@hex, a8),
      ?-,
      elem(@hex, b1),
      elem(@hex, b2),
      elem(@hex, b3),
      elem(@hex, b4),
      ?-,
      elem(@hex, c1),
      elem(@hex, c2),
      elem(@hex, c3),
      elem(@hex, c4),
      ?-,
      elem(@hex, d1),
      elem(@hex, d2),
      elem(@hex, d3),
      elem(@hex, d4),
      ?-,
      elem(@hex, e1),
      elem(@hex, e2),
      elem(@hex, e3),
      elem(@hex, e4),
      elem(@hex, e5),
      elem(@hex, e6),
      elem(@hex, e7),
      elem(@hex, e8),
      elem(@hex, e9),
      elem(@hex, e10),
      elem(@hex, e11),
      elem(@hex, e12)
    >>
  end
end
