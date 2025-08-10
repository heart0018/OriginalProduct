export type Review = {
  id: number;
  comment: string;
};

export type Card = {
  id: number;
  title: string;
  type: string;
  region: string;
  address: string;
  distance_km: number;
  rating: number;
  review_count: number;
  image_url: string;
  place_id: string;
  map_url: string;
  reviews: Review[];
};
