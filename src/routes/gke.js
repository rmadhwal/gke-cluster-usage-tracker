import {Router} from 'express';
import {plotGKE, trackGKE} from "../controllers/gkeController";

const router = Router();

router.get('/track/', async (req, res) => {
  await trackGKE(req, res)
});

router.get('/plot/', async (req, res) => {
  await plotGKE(req, res)
});

export default router;
